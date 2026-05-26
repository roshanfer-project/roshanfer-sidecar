# Useful Commands

To compile the sidecar, use:
```bash
./build.sh release
```

Set `SIDECAR_ENABLE_NANOLOG` to 1 to enable nanolog logging.

clang-tidy is optional at compile time (see `.clang-tidy`). Requires LLVM 22 (`clang-22`, `clang++-22`, `clang-tidy-22`, `libc++-22-dev`, `llvm-ar-22`). On Ubuntu 24.04 use [apt.llvm.org](https://apt.llvm.org/) (`llvm-toolchain-noble-22`). Scope: `src/*.cc` and `include/*.hpp` only (not `.c` or C `.h` headers).

C++ headers use `.hpp`; C headers remain `.h` (`tdigest.h`, `picohttpparser.h`).

`SIDECAR_CLANG_TIDY=1 ./build.sh <type>` runs clang-tidy on every C++ compile.

`SIDECAR_CLANG_TIDY_FIX=1 ./build.sh <type>` enables clang-tidy `-fix` (implies tidy; use version control first).

Builds use `-j$(nproc)` by default (including clang-tidy check/fix); set `JOBS` to override (e.g. `JOBS=4 SIDECAR_CLANG_TIDY=1 ./build.sh release`).

# Project Instructions

## Role

Act as a System's PhD student. This means we want fast prototyping of ideas without meeting industry's standard (we are not making a product). However, you should care about performance (code that runs fast and has low overhead) and reliability (prevent bugs and crashes).

## General preferences

1. Minimal and simple implementations (fewer abstractions, prioritizing understandibity).
2. Make reports compact. This makes it easier for me to review the changes. If needed, I will ask for an extended explanation.
3. Ask questions when uncertain or before applying substantial changes.



## Guidlines for speciifc scenarios

### Debugging

When the prompt is about fixing and issue or bug, you should generally apply very few changes to fix the bug and avoid chainging the strucutres, abstractions, etc. If you have to make a lot of changes, consult your plan with me before applying changes.

### Implementing new features or extneding existing ones

In these scenarios, you should always reuse as much as code possible from the existing codebase.


### When the prompt is asking a question

In these scenarios, try to teach the broader concept that lead to the question. For example, if the question is about why a particalur type casting in C++ doesn't work, you should also include a brief note about how that particular casting works.


# Input and Context

- user prompt
- the documents described in `docs` directory


# Output and Planning

The agent should work in two phases: planning and execution.

## Planning phase
The agent consumes all the context and comes up with a plan to fulfill user's request.
The agent should present the summary of the plan along with a summary of the context consumed (including a confirmation that it has read this file) to the user for confirmation.
If the agent is uncertain about parts of the plan, it should ask its questions as well.

The agent should reperat this phase until its certain.


## Execution phase
The agent simply executes the agreed plan.

At the end, it should present a summary of changes to the user.

**Make sure you are abiding by coding standards in `coding_standards.md`**