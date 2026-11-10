//go:build !race && (amd64 || arm64)

// fence_fallback.go — Go declarations for the assembly fences.
// The .s files define the symbols; these declarations make them callable
// from Go source. (A .s symbol is callable without a declaration only via
// func() assembly stubs; declaring the func here links the asm body.)
//
// NOTE: with Go 1.23 tooling, an empty Go func declaration IS the link
// mechanism for an assembly-implemented function.

package bigbuff

func fenceAcquire64GLM()

func fenceRelease64GLM()

func spinHint64GLM()
