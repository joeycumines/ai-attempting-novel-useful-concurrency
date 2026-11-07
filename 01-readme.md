# Model benchmark: Novel concurrency implementations

N.B. If you're reading this as a gist it is also an actual [repository](https://github.com/joeycumines/ai-attempting-novel-useful-concurrency).

## Introduction

This gist contains a single ad-hoc benchmark and results for a prompt I found particularly interesting.
In brief, I concluded providing AI with diverse inputs and challenging highly-technical problems is a good way to evaluate its capabilities.
This is particularly true in limited (cheap or free) in-browser chat or basic cloud agent contexts, where input contexts are strictly limited.
This gist documents the prompt, the model's responses, and my own evaluation of the results produced.

## Background

I've produced (manually, for the record) a distinctive implementation technique for synchronous broadcast in Go:

1. [ChanCaster](https://github.com/joeycumines/go-bigbuff/blob/04f9b9f58d17938686a4d5014f8ed5531c97ac34/chancaster.go)
2. [ChanPubSub](https://github.com/joeycumines/go-bigbuff/blob/04f9b9f58d17938686a4d5014f8ed5531c97ac34/chanpubsub.go)

While the core patterns - synchronous channel rendezvous, fan-out, registration, and wait-for-all synchronization - are longstanding concurrency patterns,
the implementation/design is (as far as I am aware) entirely novel within the Go ecosystem.

The interesting things these bring to the table are:

1. A protocol construction
2. Semantics where correctness depends on an explicit registration/receive contract
3. A mechanism for safe mid-flight receiver withdrawal
4. O (1) channel/control-plane state
5. Compatibility with Go's ordinary channel and `select` semantics

I also performed empirical evaluation showing the memory benefit and the throughput trade-off.

## Responses

I've tried to encode the more important details like provider, model, effort, and prompt, in the filename, where possible.
Lacking in scientific rigor perhaps, but it's worth noting that models are often opaque when accessed through AI chat interfaces.
The tools available, especially the presence of a sandbox, search engine, and (in this case) pre-installed or installable Go toolchain, are also often opaque, and can have a significant impact on the results produced.

In cases where there were individual files attached, they are encoded as a txtar down the bottom of the markdown, in a code block or similar.

- [03-chatgpt-1.md](03-chatgpt-1.md)
- [04-deepseek-v4.1-flash-chat.md](04-deepseek-v4.1-flash-chat.md)
- [05-qwen-3.8-max-chat.md](05-qwen-3.8-max-chat.md)
- [06-meta-chat-probably-muse-spark-1.3.md](06-meta-chat-probably-muse-spark-1.3.md)
- [07-qwen-3.8-max-arena.md](07-qwen-3.8-max-arena.md)
- [08-muse-spark-open-arena.md](08-muse-spark-open-arena.md)
- [09-gemini-argon-medium.md](09-gemini-argon-medium.md)
- [10-gemini-argon-medium.md](10-gemini-argon-medium.md)
- [11-meta-chat-probably-muse-spark-1.3.md](11-meta-chat-probably-muse-spark-1.3.md)
- [12-openrouter-crouton-alpha-chat.md](12-openrouter-crouton-alpha-chat.md)
- [13-openrouter-crouton-alpha-chat.md](13-openrouter-crouton-alpha-chat.md)
- [14-xing4.0-29b-a4b.md](14-xing4.0-29b-a4b.md)
- [15-nemotron-super-vl.md](15-nemotron-super-vl.md)
- [16-deepseek-v4.1-flash-chat.md](16-deepseek-v4.1-flash-chat.md)
- [17-gemini-argon-medium.md](17-gemini-argon-medium.md)
- [18-chatgpt-2.md](18-chatgpt-2.md)
- [19-chatgpt-3.md](19-chatgpt-3.md)
- [20-arena-agent.md](20-arena-agent.md)
- [21-claude-haiku-extra.md](21-claude-haiku-extra.md) ([link](https://claude.ai/share/bddce53b-2891-4698-95c7-b3af42f46dfd))
- [22-chatgpt.md](22-chatgpt.md) ([link](https://chatgpt.com/share/6aee742e-fdc0-83ec-bb9c-7227206d2528))
- [23-meta-ai.md](23-meta-ai.md)
- [24-meta-ai-prompt-mini.md](24-meta-ai-prompt-mini.md) ([link](https://meta.ai/share/TiLaDoJVDD))
- [25-claude-haiku-extra-prompt-mini.md](25-claude-haiku-extra-prompt-mini.md) ([link](https://claude.ai/share/b7f1326f-456f-4807-8d24-f77aed549dac))
- [26-kimi-ai-instant-prompt-mini.md](26-kimi-ai-instant-prompt-mini.md)
- [27-mistral-ai-chat-prompt-mini.md](27-mistral-ai-chat-prompt-mini.md)
- [28-openrouter-chat-flint-free-prompt-mini.md](28-openrouter-chat-flint-free-prompt-mini.md)
- [29-openrouter-chat-nemotron-3.5-super-vl-free-prompt-mini.md](29-openrouter-chat-nemotron-3.5-super-vl-free-prompt-mini.md)
- [30-openrouter-chat-dots3-note-preview-free-prompt-mini.md](30-openrouter-chat-dots3-note-preview-free-prompt-mini.md)
- [31-openrouter-chat-nemotron-3-ultra-free-prompt-mini.md](31-openrouter-chat-nemotron-3-ultra-free-prompt-mini.md)
- [32-openrouter-chat-nemotron-3-nano-omni-free-prompt-mini.md](32-openrouter-chat-nemotron-3-nano-omni-free-prompt-mini.md)
- [33-openrouter-chat-north-mini-code-free-prompt-mini.md](33-openrouter-chat-north-mini-code-free-prompt-mini.md)
- [34-openrouter-chat-nemotron-3.5-lightning-free-prompt-mini.md](34-openrouter-chat-nemotron-3.5-lightning-free-prompt-mini.md)
- [35-chatjimmy-prompt-mini.md](35-chatjimmy-prompt-mini.md) (Generated in 0.149s)
- [36-meta-ai-prompt-mini.md](36-meta-ai-prompt-mini.md)
- [37-gemini-3.8-flash-high-prompt-mini.md](37-gemini-3.8-flash-high-prompt-mini.md)
- [38-chatgpt-prompt-mini.md](38-chatgpt-prompt-mini.md) ([link](https://chatgpt.com/share/6aee8fc4-aad0-83ec-9cdc-d07e45cb9fab))
- [39-arena-mistral-large-4-high-prompt-mini.md](39-arena-mistral-large-4-high-prompt-mini.md)
- [40-arena-gemini-3.8-flash-high-prompt-mini.md](40-arena-gemini-3.8-flash-high-prompt-mini.md)
- [41-arena-claude-sonnet-5.5-high-prompt-mini.md](41-arena-claude-sonnet-5.5-high-prompt-mini.md)
- [42-arena-muse-spark-1.3-xhigh-prompt-mini.md](42-arena-muse-spark-1.3-xhigh-prompt-mini.md)
- [43-grok-chat-prompt-mini.md](43-grok-chat-prompt-mini.md)
- [44-qwen-chat-qwen-3.7-max-prompt-mini.md](44-qwen-chat-qwen-3.7-max-prompt-mini.md)
- [45-chatgpt-prompt-mini.md](45-chatgpt-prompt-mini.md)
- [46-arena-grok-4.6-medium-prompt-mini.md](46-arena-grok-4.6-medium-prompt-mini.md)
- [47-arena-gemini-3.8-flash.md](47-arena-gemini-3.8-flash.md)
- [48-grok-chat.md](48-grok-chat.md)
- [49-qwen-chat-qwen-3.7-max.md](49-qwen-chat-qwen-3.7-max.md)
- [50-meta-ai.md](50-meta-ai.md)
- [51-deepseek-flash-chat.md](51-deepseek-flash-chat.md)
- [52-arena-claude-opus-5-medium.md](52-arena-claude-opus-5-medium.md)
- [53-arena-gpt-6-sol.md](53-arena-gpt-6-sol.md)
- [54-arena-gpt-6.1-terra-medium-prompt-safe.md](54-arena-gpt-6.1-terra-medium-prompt-safe.md)
- [55-arena-claude-haiku-5.5-high-prompt-safe.md](55-arena-claude-haiku-5.5-high-prompt-safe.md)
- [56-github-cloud-mai-code-1.1-flash-high.md](56-github-cloud-mai-code-1.1-flash-high.md)
