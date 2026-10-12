# Product map

Planned work, in order. Each phase ships as one or more PRs.

## 1. Condition standardisation (in progress)

Structured disease/condition codes per study, with ICD-10-CM as the primary code system.

1. **Deterministic coding** (done, #5): versioned reference layer (ICD-10-CM tables, MONDO and Wikidata MeSH crosswalks, curated overrides) and a `study_condition_codes` table built during Build, with provenance and match type.
2. **API, dashboard and checks**: ICD-10 chapter/category charts, an `icd10=` filter, `make verify` checks for code validity and coverage.
3. **UMLS layer**: MeSH → UMLS CUI → ICD-10-CM and the NLM SNOMED CT → ICD-10-CM map. Licensed mapping tables stay in the private data bucket.
4. **LLM evaluation harness**: about 300 reviewed studies, `make llm-eval`, scorecard (valid codes, category/exact agreement, correct "no code", cost).
5. **LLM tier**: protocol-aware coding (title, summary, eligibility, keywords, interventions) that selects only from retrieved candidate codes, with quoted evidence, confidence, model and prompt version stored.

## 2. Treatment standardisation (after phase 1)

Structured coding of the interventions/molecules tested in each study.

Current state: `study_interventions` holds free-text type/name/description and `intervention_mesh_terms` holds derived MeSH terms (present for ~54% of Phase 1 studies); there are no drug identifiers and no intervention roles. About 44% of Phase 1 studies name a drug only by a research code (e.g. "AZD1234").

1. **Arms and roles**: store arm groups and link each intervention to its arms, giving every intervention a role (investigational, active comparator, placebo, background).
2. **Molecule reference layer**: deterministic name/synonym matching against ChEMBL, NCI Thesaurus, FDA UNII/GSRS and RxNorm (plus UMLS once licensed), producing a `study_intervention_codes` table and a molecule reference table with modality, mechanism and target. Sponsor research codes are kept when no canonical record exists. DrugBank is excluded (non-commercial licence); ATC needs a WHO licence check before public use.
3. **LLM tier**: reuses the phase 1 evaluation harness to code research-code molecules, modality and stated target from protocol text.
4. **API and dashboard**: filters and charts by molecule, mechanism/target and modality.

## 3. Patient-matching readiness (after phase 2)

Make the database usable by pre-screening/recruiting agents that match a patient's diagnoses to recruiting studies. Condition codes are a pre-screen only; final eligibility stays with the study team.

1. **Recruiting-status freshness**: refresh recruiting studies more often than weekly and re-check a study's status live before any contact.
2. **Structured eligibility**: extract inclusion/exclusion criteria into fields (age/sex, biomarkers and mutations, prior lines of therapy, ECOG, key exclusions such as brain metastases), using the same evaluation-harness approach as the condition LLM tier.
3. **Location matching**: distance from the patient to recruiting sites (lat/long is already in `study_locations`).
4. **Agent-facing API**: queries such as "recruiting trials for ICD-10-CM C34.x within 100 km of X", returning match reasons and the evidence behind each code.
5. **Licensing and compliance review** before commercial use: per-source licence status in the reference metadata so restricted sources (UMLS/SNOMED CT, ChEMBL share-alike, ATC) can be excluded; patient-data handling (HIPAA/GDPR, consent), IRB-approved recruitment materials, and referral-fee/anti-kickback rules. The database itself holds no patient data.
