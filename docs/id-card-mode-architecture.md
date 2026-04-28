# ID Card Mode Architecture

Use one shared institute entry page. Do not create separate browser pages for each institute type.

Keep institute-specific behavior in small mode handlers so rules do not mix inside the shared page.

Current mode handler:
- `static/js/idcard/modes/school.js` handles school group, school name, school address, student fields, and teacher fields.

Future mode handlers, when each area is stable:
- `static/js/idcard/modes/college.js` for college student and teacher rules.
- `static/js/idcard/modes/anm.js` for ANM trainee rules.
- `static/js/idcard/modes/department.js` for government department employee rules.
- `static/js/idcard/modes/facility.js` for block and village health facility rules.

All mode handlers should feed the same backend save/load pipeline with a common payload shape. Add or move one mode at a time, then verify save, retrieve, admin filtering, Studio preview, and print/export before moving the next mode.

