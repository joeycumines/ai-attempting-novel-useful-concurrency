# Model benchmark: Novel concurrency implementations

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

1. [03-chatgpt-1.md](03-chatgpt-1.md)
2. [04-deepseek-v4.1-flash-chat.md](04-deepseek-v4.1-flash-chat.md)
3. [05-qwen-3.8-max-chat.md](05-qwen-3.8-max-chat.md)
4. [06-meta-chat-probably-muse-spark-1.3.md](06-meta-chat-probably-muse-spark-1.3.md)
5. [07-qwen-3.8-max-arena.md](07-qwen-3.8-max-arena.md)
6. [08-muse-spark-open-arena.md](08-muse-spark-open-arena.md)
7. [09-gemini-argon-medium.md](09-gemini-argon-medium.md)
8. [10-gemini-argon-medium.md](10-gemini-argon-medium.md)
9. [11-meta-chat-probably-muse-spark-1.3.md](11-meta-chat-probably-muse-spark-1.3.md)
10. [12-openrouter-crouton-alpha-chat.md](12-openrouter-crouton-alpha-chat.md)
11. [13-openrouter-crouton-alpha-chat.md](13-openrouter-crouton-alpha-chat.md)
12. [14-xing4.0-29b-a4b.md](14-xing4.0-29b-a4b.md)
13. [15-nemotron-super-vl.md](15-nemotron-super-vl.md)
14. [16-deepseek-v4.1-flash-chat.md](16-deepseek-v4.1-flash-chat.md)
15. [17-gemini-argon-medium.md](17-gemini-argon-medium.md)
16. [18-chatgpt-2.md](18-chatgpt-2.md)
17. [19-chatgpt-3.md](19-chatgpt-3.md)
18. [20-arena-agent.md](20-arena-agent.md)
19. [21-claude-haiku-extra.md](21-claude-haiku-extra.md) ([link](https://claude.ai/share/bddce53b-2891-4698-95c7-b3af42f46dfd))
20. [22-chatgpt.md](22-chatgpt.md) ([link](https://chatgpt.com/share/6aee742e-fdc0-83ec-bb9c-7227206d2528))
21. [23-meta-ai.md](23-meta-ai.md)
22. [24-meta-ai-prompt-mini.md](24-meta-ai-prompt-mini.md) ([link](https://meta.ai/share/TiLaDoJVDD))
