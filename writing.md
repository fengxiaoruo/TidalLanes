# Writing Style Guide — Urban Economics / Spatial Economics

This file is the single source of truth for written output in the TidalLanes project. It applies to any text a model produces in this repo: LaTeX prose in the paper, abstracts and slide text, referee responses, memos, and long-form documentation. Every agent (Claude and any subagent) must read this file before generating English prose for the project and must conform to it strictly.

It does *not* govern code comments, commit messages, or chat-style replies back to the user.

---

## 0. Reading order

Before writing, the agent reads this file in full and then reads (or re-reads, if the task is revision) the target document. No English prose is generated without this step.

If a project-level instruction (for example in `AGENTS.md` or `nextstep.md`) conflicts with this file, this file governs.

---

## 1. Core stylistic mandates

### 1.1 Spatial and economic precision

Avoid vague descriptions of growth, change, or activity. Name the economic object. Prefer the technical term to a paraphrase.

Preferred vocabulary: *agglomeration economies, centripetal and centrifugal forces, spatial equilibrium, idiosyncratic preferences, comparative statics, general equilibrium, reduced form, structural estimand, market access, sorting, spillover, congestion externality, iceberg cost, exact-hat algebra*. See Section 5 for a fuller list.

### 1.2 Non-symmetrical syntax

Avoid the two-beat balanced cadence that gives AI prose its distinctive rhythm. Examples of the pattern to avoid:

- "While cities offer X, they also present Y."
- "On the one hand A. On the other hand B."
- "Not only P, but also Q."

Vary sentence length. Use subordinating conjunctions, apposition, and left-branching clauses. A short declarative sentence ("The externality is directional.") is often the most effective contrast to a paragraph of long ones.

### 1.3 Punctuation

- **Em-dashes (—) and LaTeX `---`: strictly forbidden.** Replace with a comma-plus-appositive, a parenthetical, or, where the clauses are independent, a semicolon.
- **En-dashes (LaTeX `--`)**: only for numerical ranges (`2010--2019`) and compound adjectives such as `origin--destination`.
- **Semicolons**: only to link two independent clauses with a contrastive or parallel logical relation. Never as a fancier comma.
- **Parentheses**: preferred over dashes for non-restrictive insertions.
- **Footnotes**: for qualifications that would otherwise interrupt a quantitative sentence. Do not use footnotes for literature asides.

---

## 2. Forbidden phrases and transitions

The following are AI "fingerprints" and must not appear in prose:

- "It is worth noting (that)…"
- "It is important to note…"
- "It should be noted…"
- "Crucially, …" / "Crucial"
- "Moreover," at the start of a sentence
- "Furthermore," at the start of a sentence
- "In conclusion, …"
- "A testament to…"
- "Delve into…"
- "In the realm of…"
- "Navigate the complexities of…"
- "The landscape of…"
- "Tapestry of…"
- "At the heart of…"

### 2.1 Verb substitutions

The left column is the prose default for AI text. Use the right column, except where the left-hand form is a fixed usage in a proof environment or a technical clause.

| Avoid | Prefer |
|---|---|
| show / demonstrate (in prose) | establish, deliver, imply, evince |
| improve | ameliorate, raise, reduce, tighten, sharpen |
| big, large (in prose) | substantial, nontrivial, first-order, economically meaningful |
| small (in prose) | modest, second-order, quantitatively limited |
| a lot of | substantial, considerable |
| use (where precise) | exploit, deploy, leverage (only if accurate) |

Exceptions retained intact:

- "prove" in a proof environment or in "Proposition / Lemma … proved in Appendix X".
- "large" / "small" when describing a parameter or object where the adjective is a technical statement (for example "$\theta$ large", "the largest weakly connected component of $(\N,\E)$").
- "show" in standard regression description ("Column (3) shows the IV estimate") is acceptable if used sparingly.

---

## 3. Discipline-specific logic

### 3.1 Hedging and causal language

Claims about empirical results are hedged in line with the identification that actually supports them.

Avoid: "The policy causes gentrification."

Prefer: "The point estimate is consistent with a positive rent response to the treatment, under the parallel-trends assumption documented in Section X."

Causal language is reserved for (i) results that hold by construction, (ii) proofs of theoretical propositions, and (iii) reduced forms whose identification strategy is explicitly named (RD, IV with a stated exclusion restriction, difference-in-differences with pre-trends shown, shift-share with a defensible instrument).

When citing results from other papers, retain the hedging used in the original. Do not promote a conditional claim in the source to an unconditional claim in our text.

### 3.2 Mechanism-first paragraph structure

Do not open a paragraph with a general framing sentence that signposts what is about to happen. Open with the mechanism, the primitive, or the empirical anomaly.

Preferred shape:

1. Mechanism, primitive, or empirical fact, stated plainly.
2. The quantitative object it produces (the equation, the moment, the estimand, the counterfactual).
3. The consequence that links to the next paragraph.

Where the theoretical frame is needed (Alonso-Muth-Mills, Rosen-Roback spatial equilibrium, quantitative spatial model à la Ahlfeldt et al. 2015 or Allen-Arkolakis, gravity with Fréchet preferences), it enters in step 2 as the apparatus, not in step 1 as an invocation.

### 3.3 Spatial interaction

Urban-economics writing reflects the interdependencies of space. Name them explicitly: *spillover, negative externality, sorting, reallocation, congestion feedback, market access, general-equilibrium adjustment*. Avoid phrasing that collapses these into single-cause stories.

Do not overstate the direction of causality when the mechanism is a fixed point. For example, prefer "workplace productivity and residential amenities are jointly determined with commuting flows in the equilibrium system" over "agglomeration makes firms more productive, which attracts workers, which raises amenities".

---

## 4. Structural constraints

### 4.1 No sandwich paragraphs

Avoid the three-step "topic sentence → example → restatement" shape. The last sentence should advance the argument, not restate the first sentence.

### 4.2 Active versus passive

- Active voice for theoretical propositions and model predictions: "The model predicts…", "The inversion recovers…", "Proposition 2 establishes…".
- Passive voice for data construction and econometric procedure: "Standard errors are clustered at the census-tract level.", "The sample is restricted to the morning peak (07:00--09:00).", "Directed speeds are length-weighted harmonic averages of centreline observations."

### 4.3 Paragraph length

Each paragraph contains one load-bearing claim. If a paragraph contains more than one, split it. Do not produce paragraphs longer than roughly twelve lines of typeset output; the paper's reader is assumed to be tired and to be scanning for equations.

### 4.4 Display equations

An equation is introduced by a sentence ending in a comma or colon, never by a trailing "the following". Refer back by `\eqref{...}` rather than by "the above equation".

---

## 5. Discipline-specific vocabulary

Preferred names. Use these, not casual paraphrases.

**Spatial structure.** Monocentric and polycentric city; central business district (CBD); bid-rent curve; Alonso-Muth-Mills (AMM) framework; quantitative spatial model (QSM); reduced-form urban model.

**Agglomeration and dispersion.** Agglomeration economies; Marshallian externalities; Marshall-Arrow-Romer (MAR) versus Jacobs spillovers; urbanisation versus localisation economies; centripetal and centrifugal forces; Rosen-Roback spatial equilibrium; sorting on amenities; amenity capitalisation.

**Frictions and flows.** Commuting gravity; iceberg cost; Fréchet dispersion; route choice; link traffic; resident market-access index; firm market-access index; bilateral commuting cost.

**Congestion and transport.** Speed-flow relation; Bureau of Public Roads (BPR) function; congestion elasticity; fundamental law of road congestion (Duranton-Turner); tidal-lane (contraflow) reallocation; directional capacity; free-flow speed; lane-kilometre.

**Econometric framing.** Reduced form; structural estimation; exact-hat algebra; welfare decomposition; counterfactual equilibrium; inversion of fundamentals; externally calibrated parameter; internally estimated parameter; identification-at-infinity; spatial-first-difference.

**Identification.** Exclusion restriction; first-stage F-statistic; weak-instrument-robust inference; monotonicity; local average treatment effect (LATE); pre-trends; placebo; shift-share; Bartik; Borusyak-Hull-Jaravel exposure design.

[Add domain-specific placeholders here as the project grows. Leave entries blank rather than inventing names.]

---

## 6. Citation and bibliography conventions (this project)

- Use `\citet{key}` for textual citations ("Ahlfeldt et al.\ (2015) obtain…") and `\citep{key}` for parenthetical citations.
- The `natbib` package must be loaded in every paper `.tex` file. Compilability is separately enforced.
- Every cite key must exist in the project `.bib` file or in the in-file `thebibliography`.
- Do not add a citation unless the cited result is actually used in the sentence where it appears.

---

## 7. Handling uncertainty

When unsure of an exact fact (a specific elasticity, a paper's numerical result, a policy date), do not fill with a plausible invention. Leave an explicit marker:

```
[TODO: verify residential-congestion elasticity in Monte, Redding and Rossi-Hansberg (2018)]
```

A visible placeholder is strictly preferred to confident filler.

---

## 8. Final output filter

Before returning prose, the agent runs the following checklist. Any item that fails is fixed before output.

1. Zero em-dashes (—) and zero LaTeX `---`.
2. Zero forbidden AI fingerprints (Section 2).
3. *show / prove / improve / big / large* replaced in prose, retained only in technical or proof-environment usage.
4. Causal claims hedged to match the identification that supports them.
5. No sandwich paragraphs. Each paragraph's last sentence advances the argument.
6. Terminology matches Section 5.
7. Active voice for theory; passive voice for data and procedure.
8. For LaTeX files: compiles cleanly with zero undefined references, citations, or control sequences.

Do not include conversational filler, acknowledgements, or meta-commentary about the task. Begin immediately with the requested text.
