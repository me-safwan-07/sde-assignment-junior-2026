# AI Usage Disclosure

AI tools are allowed. We want to know how they were used, because using them well is a skill.

## Tools used
- Copilot SDK in VS Code

## What I used them for
- Reviewed the assignment requirements and data model.
- Helped reason through the lead-validation rules, duplicate-merging behaviour, and event-linking logic.
- Drafted the initial pipeline structure and verification tests.

## What I wrote or decided myself
- The final SQLite schema and ingestion logic.
- The validation and normalisation rules for phone numbers, dates, decimals, and enums.
- The final report-generation rules and the idempotent ingest strategy.

## Where the AI was wrong or unhelpful
- The first draft overcomplicated persistence and report generation. I rejected that version and simplified it to a direct SQLite-backed pipeline.
- Some timestamp formatting logic was initially wrong; I corrected it by checking the generated ISO outputs against the contract.

## How I verified AI-generated code
- Ran the repo's pytest suite covering phone/date parsing and a full ingest + report cycle.
- Executed the generator and the CLI against a small seeded dataset and inspected the output files directly.
