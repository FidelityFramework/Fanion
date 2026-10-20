# Request-scoped graph constraints

The native retrieval gateway owns the query grammar. `Gbnf.fs` generates it from the live schema and permitted seed IDs. Keep grammar construction and query validation in that owner.

For Magnitude, send the generated grammar immediately as `response_format: {type: "grammar", grammar: ...}` together with `reasoning_effort: "none"`. The native `magnitude_gbnf` profile requires thinking explicitly false and rejects responses reporting reasoning output. Do not substitute a model registry flag for that request constraint.

A grammar constrains output shape and permitted values. It does not prove that the selected relationship answers the question. Validate seed, edge kind, direction, returned identities, pagination and hydrated sources against the graph.

The grammar, messages and complete request have hashes in the gateway receipt. Preserve those bindings when reporting a result. Do not hand-maintain a second copy of the generated grammar in a client.

GBNF character classes contain Unicode scalars. Do not emit surrogate-codepoint
escapes in those classes; the JSON escape rule separately rejects lone surrogate
escapes. Startup priming exercises seed discovery as well as query generation.
