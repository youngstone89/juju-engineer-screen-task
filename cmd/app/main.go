package main

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"
)

// MetricEnvelope represents the envelope structure from the API
type MetricEnvelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// LoadPayload structure for "load_avg" type
type LoadPayload struct {
	Value float64 `json:"value"`
}

// CpuPayload structure for "cpu_usage" type
type CpuPayload struct {
	Value []float64 `json:"value"`
}

// KernelPayload structure for "last_kernel_upgrade" type
type KernelPayload struct {
	Value int64 `json:"value"`
}

// DataIngester fetches metrics from the API and sends them to the dispatcher
type DataIngester struct {
	client       *http.Client
	url          string
	outChan      chan<- MetricEnvelope
	done         chan struct{}
	pollInterval time.Duration
}

func NewDataIngester(outChan chan<- MetricEnvelope, pollInterval time.Duration) *DataIngester {
	return &DataIngester{
		client:       &http.Client{Timeout: 10 * time.Second},
		url:          "http://localhost:8080/metrics",
		outChan:      outChan,
		done:         make(chan struct{}),
		pollInterval: pollInterval,
	}
}

func (di *DataIngester) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("DataIngester restarted after panic: %v", r)
				di.Start()
			}
		}()
		ticker := time.NewTicker(di.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				resp, err := di.client.Get(di.url)
				if err != nil {
					log.Printf("Error fetching metrics: %v", err)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					log.Printf("Unexpected status code: %d", resp.StatusCode)
					resp.Body.Close()
					continue
				}
				var envelopes []MetricEnvelope
				if err := json.NewDecoder(resp.Body).Decode(&envelopes); err != nil {
					log.Printf("Error decoding response: %v", err)
					resp.Body.Close()
					continue
				}
				resp.Body.Close()
				for _, env := range envelopes {
					di.outChan <- env
				}
			case <-di.done:
				return
			}
		}
	}()
}

func (di *DataIngester) Stop() {
	close(di.done)
}

// Dispatcher routes envelopes to the correct handler
type Dispatcher struct {
	inChan       <-chan MetricEnvelope
	handlers     map[string]handlerConfig
	registerChan chan handlerRegistration
	done         chan struct{}
}

type handlerConfig struct {
	ch     chan<- interface{}
	parser func(json.RawMessage) (interface{}, error)
}

type handlerRegistration struct {
	typeStr string
	ch      chan<- interface{}
	parser  func(json.RawMessage) (interface{}, error)
}

func NewDispatcher(inChan <-chan MetricEnvelope) *Dispatcher {
	return &Dispatcher{
		inChan:       inChan,
		handlers:     make(map[string]handlerConfig),
		registerChan: make(chan handlerRegistration),
		done:         make(chan struct{}),
	}
}

func (d *Dispatcher) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Dispatcher restarted after panic: %v", r)
				d.Start()
			}
		}()
		for {
			select {
			case reg := <-d.registerChan:
				d.handlers[reg.typeStr] = handlerConfig{reg.ch, reg.parser}
			case env, ok := <-d.inChan:
				if !ok {
					return
				}
				hc, exists := d.handlers[env.Type]
				if !exists {
					log.Printf("No handler for type %s", env.Type)
					continue
				}
				parsed, err := hc.parser(env.Payload)
				if err != nil {
					log.Printf("Error parsing %s: %v", env.Type, err)
					continue
				}
				hc.ch <- parsed
			case <-d.done:
				return
			}
		}
	}()
}

func (d *Dispatcher) Register(typeStr string, ch chan<- interface{}, parser func(json.RawMessage) (interface{}, error)) {
	d.registerChan <- handlerRegistration{typeStr, ch, parser}
}

func (d *Dispatcher) Stop() {
	close(d.done)
}

// LoadHandler processes "load_avg" payloads
type LoadHandler struct {
	inChan chan interface{}
	min    float64
	max    float64
	done   chan struct{}
}

func NewLoadHandler() *LoadHandler {
	return &LoadHandler{
		inChan: make(chan interface{}, 10),
		min:    math.MaxFloat64,
		max:    0,
		done:   make(chan struct{}),
	}
}

func (lh *LoadHandler) InputChan() chan<- interface{} {
	return lh.inChan
}

func (lh *LoadHandler) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("LoadHandler restarted after panic: %v", r)
				lh.Start()
			}
		}()
		for {
			select {
			case p := <-lh.inChan:
				v, ok := p.(LoadPayload)
				if !ok {
					log.Fatal("failed to cast to LoadPaylod: %v", p)
					continue
				}
				if v.Value < lh.min {
					lh.min = v.Value
				}
				if v.Value > lh.max {
					lh.max = v.Value
				}
				log.Printf("Load: min=%.2f, max=%.2f", lh.min, lh.max)
			case <-lh.done:
				return
			}
		}
	}()
}

func (lh *LoadHandler) Stop() {
	close(lh.done)
}

// CPUHandler processes "cpu_usage" payloads
type CPUHandler struct {
	inChan   chan interface{}
	averages []float64
	counts   []int
	done     chan struct{}
}

func NewCPUHandler() *CPUHandler {
	return &CPUHandler{
		inChan: make(chan interface{}, 10),
		done:   make(chan struct{}),
	}
}

func (ch *CPUHandler) InputChan() chan<- interface{} {
	return ch.inChan
}

func (ch *CPUHandler) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("CPUHandler restarted after panic: %v", r)
				ch.Start()
			}
		}()
		for {
			select {
			case p := <-ch.inChan:
				v, ok := p.(CpuPayload)
				if !ok {
					log.Fatalf("failed to cast type to CpuPayload :%v", p)
					continue
				}

				if ch.averages == nil {
					ch.averages = make([]float64, len(v.Value))
					ch.counts = make([]int, len(v.Value))
				}
				for i, val := range v.Value {
					ch.counts[i]++
					ch.averages[i] += (val - ch.averages[i]) / float64(ch.counts[i])
				}
				log.Printf("CPU Averages: %v", ch.averages)
			case <-ch.done:
				return
			}
		}
	}()
}

func (ch *CPUHandler) Stop() {
	close(ch.done)
}

// KernelHandler processes "last_kernel_upgrade" payloads
type KernelHandler struct {
	inChan chan interface{}
	latest int64
	done   chan struct{}
}

func NewKernelHandler() *KernelHandler {
	return &KernelHandler{
		inChan: make(chan interface{}, 10),
		done:   make(chan struct{}),
	}
}

func (kh *KernelHandler) InputChan() chan<- interface{} {
	return kh.inChan
}

func (kh *KernelHandler) Start() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("KernelHandler restarted after panic: %v", r)
				kh.Start()
			}
		}()
		for {
			select {
			case p := <-kh.inChan:
				v, ok := p.(KernelPayload)
				if !ok {
					log.Fatalf("failed to cast type to KernelPayload :%v", p)
					continue
				}
				if v.Value > kh.latest {
					kh.latest = v.Value
				}
				log.Printf("Last Kernel Upgrade: %v", time.Unix(kh.latest, 0))
			case <-kh.done:
				return
			}
		}
	}()
}

func (kh *KernelHandler) Stop() {
	close(kh.done)
}

// Parsers for each payload type
func parseLoadPayload(data json.RawMessage) (interface{}, error) {
	var p LoadPayload
	err := json.Unmarshal(data, &p)
	return p, err
}

func parseCPUPayload(data json.RawMessage) (interface{}, error) {
	var p CpuPayload
	err := json.Unmarshal(data, &p)
	return p, err
}

func parseKernelPayload(data json.RawMessage) (interface{}, error) {
	var p KernelPayload
	err := json.Unmarshal(data, &p)
	return p, err
}

func main() {
	ingesterToDispatcher := make(chan MetricEnvelope, 100)
	ingester := NewDataIngester(ingesterToDispatcher, 2*time.Second)
	ingester.Start()

	dispatcher := NewDispatcher(ingesterToDispatcher)
	dispatcher.Start()

	loadHandler := NewLoadHandler()
	dispatcher.Register("load_avg", loadHandler.InputChan(), parseLoadPayload)

	cpuHandler := NewCPUHandler()
	dispatcher.Register("cpu_usage", cpuHandler.InputChan(), parseCPUPayload)

	kernelHandler := NewKernelHandler()
	dispatcher.Register("last_kernel_upgrade", kernelHandler.InputChan(), parseKernelPayload)

	loadHandler.Start()
	cpuHandler.Start()
	kernelHandler.Start()

	select {} // Block main goroutine
}
