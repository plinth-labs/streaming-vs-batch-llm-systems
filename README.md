# Streaming vs Batch LLM Systems
This repository provides a comprehensive blog series and reference materials on designing and operating streaming vs. batch inference systems for large language models (LLMs). Streaming (low-latency) systems deliver tokens to users in real time (e.g. via Server-Sent Events or WebSockets), while batch (high-throughput) systems process large datasets offline. Each approach has different goals and trade-offs: streaming optimizes interaction latency, whereas batch maximizes throughput and GPU efficiency. This repo is organized into folders for each blog post and accompanying code snippets, serving as a one-stop research hub for engineers building LLM serving pipelines.


# Proposed Sections

### Streaming vs. Batch LLM Inference: Interaction vs. Throughput
This section will introduce the core dichotomy as a matter of interactive latency vs. throughput. We’ll contrast real-time “chat” style APIs (where users see tokens appear as generated) with offline batch jobs (processing many inputs together). Key metrics such as Time-to-First-Token (TTFT) and Time-Per-Token (TPOT) versus overall tokens-per-second will be compared. Use cases for each mode will be highlighted (e.g. conversational agents vs. bulk content generation), and the goal of this post is to set the stage for the rest of the series.

### Real-Time LLM Services: Streaming APIs & Architectures
This section will dive into streaming inference pipelines and protocols. We’ll explain token streaming via SSE or WebSockets and why it greatly improves user experience. For example, using Server-Sent Events (SSE) avoids complex bidirectional WebSocket setups when only server→client data flow is needed. We’ll show how to wire an LLM with callback loops to emit each generated token immediately, and discuss practical considerations (connection limits, backpressure, reconnection). Topics include partial decoding (sending early prefixes) and even letting users interrupt/cancel generation. (E.g. NVIDIA’s Triton supports request cancellation so that wasted long-running LLM calls can be terminated.) A sample SSE-based Node.js snippet will illustrate token streaming, and we’ll discuss architectures (Nginx/Cloudflare configs, stateless auto-retry, etc.) gleaned from production RAG systems.

### Batch LLM Inference: High-Throughput Pipelines and GPU Utilization
This section will cover batch-oriented pipelines for LLMs, which use queues (Kafka, SQS) and micro-batching to maximize GPU throughput. We’ll describe queue-driven processing: e.g. front-end services enqueue LLM requests into Kafka, and a fleet of workers pulls messages to form batches. We’ll explain how queuing systems offer persistence and replay (critical for reliability). The focus will then shift to GPU optimization: how to pack many prompts into each GPU batch to keep it busy. We’ll review dynamic batching techniques – in particular “continuous batching” – which refill the GPU with new sequences as old ones finish. Citing Databricks/Anyscale guidance, we’ll note that LLM inference is memory-I/O bound, so throughput is limited by GPU memory batch size. Continuous batching can yield 8–23× higher throughput over naive (static) batching by better using GPU memory. We’ll include example code (e.g. Ray Serve + vLLM) and discuss scheduling knobs for mixed workloads.

### Hybrid LLM Serving: Caching, Precomputation & Mixed Workloads
This advanced section will discuss blending real-time and batch methods. For example, you might run real-time streaming inference for interactive users and have background processes to update context or run batch inference on new data. We’ll cover caching strategies (KV-cache reuse between requests to speed repeated prompts) and prefetching (computing parts of responses or embeddings offline). Research systems like BROS show how to schedule real-time (RT) vs best-effort (BE) requests on the same machines. BROS achieved up to 74% lower latency on RT queries with negligible impact on BE throughput by priority scheduling and sharing the model’s KV cache between request types. We’ll also describe approaches like context streaming (feeding new retrieved context in chunks to the model) which can reduce TTFT by 4–11× over waiting for full retrieval. The folder will include scripts for simulating hybrid scheduling (e.g. combining a chat API with asynchronous batch enrichers) and tips on tuning GPU resource allocation vs. latency targets.

### Comparative Tradeoff Table
A concise table of trade-offs will summarize the above concepts for quick reference. Rows might include metrics (latency vs. throughput), typical protocols (SSE vs. batch queue), GPU utilization, and suitable use cases. Each cell will cite insights from the above resources. For example, we’ll note that streaming systems excel at low TTFT but may under-utilize GPUs unless carefully scheduled, whereas batch systems maximize tokens/sec via large batches and dynamic batching. Hybrid setups (if used) strike a balance by collocating workloads. This table will serve as a quick decision guide for engineers.


# Repository Structure & Usage
The repository is organized into the following folders, each corresponding to a blog post above:

- ```streaming-vs-batch-interaction-vs-throughput/``` – Content and code for Streaming vs Batch LLM Inference: Interaction vs Throughput. Includes an outline, diagrams comparing metrics, and reference scripts illustrating latency vs throughput.
- ```real-time-llm-services/``` – Material for Real-Time LLM Services: Streaming APIs & Architectures. Contains SSE/WebSocket demo code, examples of token-callback hooks, and notes on load-balancer configs.
- ```batch-llm-pipelines/``` – Material for Batch LLM Inference: High-Throughput Pipelines and GPU Utilization. Includes examples using Kafka or Ray Serve to drive queued inference, along with benchmarking scripts that demonstrate continuous batching speedups.
- ```hybrid-llm-serving/``` – Material for Hybrid LLM Serving: Caching, Precomputation & Mixed Workloads. Contains references to caching libraries and example orchestrations combining streaming APIs with background batch processes (e.g. context backfills).
- ```tradeoffs-table/``` – A markdown file summarizing key trade-offs across approaches, drawing on sources like Anyscale, NVIDIA, and research papers.
Each folder will contain the draft blog text (in Markdown), code snippets, and collected notes. The goal is that readers (AI/ML engineers and researchers) can browse the repo to find both high-level explanations and concrete examples for building streaming or batch LLM systems.


# Key References and Insights

### Metrics & Use Cases: 
- Online LLM APIs measure user-perceived latency (TTFT, token rates) whereas batch jobs prioritize throughput and cost efficiency.
- For example, serving chatbots needs sub-second time-to-first-token, while batch jobs (summarizing documents at scale) can take minutes per request if overall throughput is high.

### Streaming Architecture: 
- Server-Sent Events (SSE) is a simple, browser-native way to stream tokens, avoiding WebSocket complexity when only server-to-client updates are needed.
- A typical pipeline uses an LLM SDK’s token callback and pushes each token down an SSE channel (e.g., res.write("data: ...")). In production, long-lived HTTP streams must handle thousands of connections (tuned keep-alives) and client reconnects (EventSource auto-retries). Pipelines should also support request cancellation: e.g. NVIDIA Triton’s backends can abort in-progress LLM inferences to save GPU work once a client drops connection.

### Batch Pipelines & Batching: 
- Batch inference systems use queues (Kafka, SQS) to accumulate requests. Batches are formed and sent to GPUs to maximize occupancy. Dynamic batching (also called continuous or iteration-level batching) significantly outperforms fixed-size batching by replacing finished sequences with new ones on the fly.
- Anyscale benchmarks show continuous batching (e.g. via vLLM) can be 8–23× higher throughput than naive batching, while also improving median latency by allowing incoming requests to join existing batches. Key point: because LLM inference is memory-bound, throughput scales with batch size (GB of tokens per GPU).

### Hybrid Strategies: 
- Real-world systems often mix workloads. The BROS system shows one can run latency-sensitive (RT) and throughput-oriented (BE) LLM workloads on the same cluster by priority scheduling and sharing the model’s KV cache. Context streaming (from Stream2LLM) overlaps retrieval and inference: it feeds the model chunks of newly retrieved context so generation can start earlier, reducing TTFT by up to 4–11× compared to waiting for full context.
- These works highlight a GPU utilization vs. latency trade-off: batching more requests improves utilization but increases per-request delay. Intelligent schedulers manage this trade-off to meet SLAs.

### Caching & Precomputation: 
- To further optimize, systems use KV-caching (reusing prefix keys/values to skip redundant work) and offline precomputation. For example, once some tokens are generated, their KV cache can be reused if the same prompt is reused.
- Precomputing embeddings or generating partial answers for common queries can also shift work offline. While not always covered in streaming vs batch, these techniques (along with techniques like function calling in LLMs) fit naturally into hybrid pipelines.

# Getting Started
To contribute or use this repository:

- Clone and explore folders: Each folder contains a draft research post (.md), code examples, and collected references. Start with the README of each folder for context.
- Run examples: Some folders contain scripts (Python or Node.js) demonstrating streaming or batch inference setups. Install dependencies (e.g. ```npm install express```) and follow instructions in the code comments.
- Read references: The references/ or sources/ directory (if any) will hold PDFs or links cited above. They provide deeper technical background (e.g. Anyscale batch inference, continuous batching benchmarks, Triton docs, etc.).
- Use the trade-off table: The comparative table can guide architecture decisions for new projects.
- This repo is meant to evolve: as new LLM serving techniques emerge, we will update the blogs and code. We encourage issues and pull requests to refine examples or add recent findings.
- The goal is to make Streaming vs Batch LLM Systems a definitive resource for ML engineers and researchers designing LLM inference pipelines.
