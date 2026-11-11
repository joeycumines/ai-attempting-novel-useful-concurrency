package bigbuff

// The runnable registry: every top-level Test, Benchmark and Fuzz function in
// the package, with the scenarios it bears on.
//
// A runnable is exactly one of three classes.
//
//	case     registered in Cases against a scenario; it runs every attached
//	         implementation of that scenario, so its results compare them.
//	unit     lives in the test file of one implementation source file. It is
//	         registered here against that source file, the implementations
//	         the file provides, and every scenario those implementations are
//	         attached to. It exercises one implementation's own contract, so
//	         it shows that the implementation holds where it is attached; it
//	         does not compare implementations, and is not meant to.
//	harness  adapter conformance across all scenarios, or the tooling
//	         testing itself; listed in harnessRunnables with the reason.
//
// Nothing here is maintained by hand except the two small tables below; the
// rest is read from the source files, so a new test file is registered by
// existing. TestScenarioCaseRunnablesExhaustive fails if any runnable cannot
// be classified, or if a unit runnable resolves to no scenario.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type RunnableClass string

const (
	RunnableCase    RunnableClass = "case"
	RunnableUnit    RunnableClass = "unit"
	RunnableHarness RunnableClass = "harness"
)

// Runnable is one top-level Test, Benchmark or Fuzz function.
type Runnable struct {
	Name  string
	File  string
	Kind  CaseKind
	Class RunnableClass
	// Cases are the registered cases this function executes (class case).
	Cases []CaseID
	// Source is the implementation source file a unit runnable belongs to.
	Source string
	// Implementations are the attached implementations Source provides.
	Implementations []string
	// Scenarios the runnable bears on: those of its cases, or those its
	// implementations are attached to. Empty for class harness.
	Scenarios []ScenarioID
	// Reason says why a harness runnable is not tied to a scenario.
	Reason string
}

// sourceImplementations names the attached implementations of the source
// files whose declarations do not carry the attachment's name, which is how
// the others are resolved (see implementationsOf).
var sourceImplementations = map[string][]string{
	// The generic Fanout is response 45's; its adapters are named for the
	// response.
	"fanout.go": {"Fanout45ChatGPT/sync (45-ChatGPT)", "Fanout45ChatGPT/buffered (45-ChatGPT)"},
	// The bitmap kernels have no attachment of their own; the response-72
	// broadcaster calls them on every Broadcast and Subscribe.
	"bitmap_72_gemini.go": {"PhaseCaster72Gemini (72-Gemini)", "PhaseCaster72Gemini_phased (72-Gemini)"},
}

var (
	runnableFuncRE = regexp.MustCompile(`(?m)^func\s+((?:Test|Benchmark|Fuzz)[A-Za-z0-9_]*)\s*\(`)
	declaredNameRE = regexp.MustCompile(`(?m)^(?:type\s+([A-Z][A-Za-z0-9_]*)|func\s+New([A-Z][A-Za-z0-9_]*)\s*[\[(])`)
)

// attachmentBase is the identifier an attachment's display name starts with:
// "Ring62Haiku" for "Ring62Haiku (62-Haiku)", "ChanFeed" for
// "ChanFeed_block_c64", "EpochCast41Sonnet" for "EpochCast41Sonnet/lossy".
func attachmentBase(impl string) string {
	if i := strings.IndexAny(impl, " _(/"); i >= 0 {
		return impl[:i]
	}
	return impl
}

// implementationsOf returns the attached implementations a source file
// provides: those whose base name is a type the file (or a build-tagged
// sibling such as name_fast.go) declares or constructs, unless the file is
// listed in sourceImplementations.
func implementationsOf(source string, attached []string) ([]string, error) {
	if names, ok := sourceImplementations[source]; ok {
		return append([]string(nil), names...), nil
	}
	stem := strings.TrimSuffix(source, ".go")
	siblings, err := filepath.Glob(stem + "_*.go")
	if err != nil {
		return nil, err
	}
	declared := map[string]bool{}
	for _, file := range append([]string{source}, siblings...) {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		// A sibling that is itself an implementation source with its own
		// test file is a different implementation.
		if file != source {
			if _, err := os.Stat(strings.TrimSuffix(file, ".go") + "_test.go"); err == nil {
				continue
			}
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		for _, m := range declaredNameRE.FindAllSubmatch(data, -1) {
			declared[string(m[1])+string(m[2])] = true
		}
	}
	var out []string
	for _, impl := range attached {
		if declared[attachmentBase(impl)] {
			out = append(out, impl)
		}
	}
	return out, nil
}

// Runnables returns every top-level runnable of the package, sorted by file
// and name. It reads the package's source files from the working directory.
func Runnables() ([]Runnable, error) {
	casesOf := map[string][]Case{}
	for _, c := range Cases {
		casesOf[c.Runnable] = append(casesOf[c.Runnable], c)
	}
	scenariosOfImpl := map[string]map[ScenarioID]bool{}
	var attached []string
	for _, a := range allAttachments() {
		if scenariosOfImpl[a.Impl] == nil {
			scenariosOfImpl[a.Impl] = map[ScenarioID]bool{}
			attached = append(attached, a.Impl)
		}
		scenariosOfImpl[a.Impl][a.Scenario] = true
	}
	sort.Strings(attached)

	files, err := filepath.Glob("*_test.go")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	implsOfSource := map[string][]string{}
	var out []Runnable
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		source := implementationSource(file)
		for _, m := range runnableFuncRE.FindAllSubmatch(data, -1) {
			r := Runnable{Name: string(m[1]), File: file}
			switch {
			case strings.HasPrefix(r.Name, "Benchmark"):
				r.Kind = CaseBenchmark
			case strings.HasPrefix(r.Name, "Fuzz"):
				r.Kind = CaseFuzz
			default:
				r.Kind = CaseTest
			}
			reason, listed := harnessRunnables[r.Name]
			cases := casesOf[r.Name]
			seen := map[ScenarioID]bool{}
			switch {
			case len(cases) > 0 && listed:
				return nil, fmt.Errorf("%s (%s) is both a registered case and listed in harnessRunnables", r.Name, file)
			case len(cases) > 0:
				r.Class = RunnableCase
				for _, c := range cases {
					r.Cases = append(r.Cases, c.ID)
					seen[c.Scenario] = true
				}
			case listed:
				r.Class = RunnableHarness
				r.Reason = reason
			case source != "":
				r.Class = RunnableUnit
				r.Source = source
				impls, ok := implsOfSource[source]
				if !ok {
					if impls, err = implementationsOf(source, attached); err != nil {
						return nil, err
					}
					implsOfSource[source] = impls
				}
				r.Implementations = impls
				for _, impl := range impls {
					for id := range scenariosOfImpl[impl] {
						seen[id] = true
					}
				}
			default:
				return nil, fmt.Errorf("%s (%s) is not a registered case, is not in an implementation's own test file, and is not listed in harnessRunnables", r.Name, file)
			}
			for id := range seen {
				r.Scenarios = append(r.Scenarios, id)
			}
			sort.Slice(r.Scenarios, func(i, j int) bool { return r.Scenarios[i] < r.Scenarios[j] })
			out = append(out, r)
		}
	}
	return out, nil
}
