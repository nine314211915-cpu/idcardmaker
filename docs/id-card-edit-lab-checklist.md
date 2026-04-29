# ID Card Edit Lab Checklist

Status legend:
- `[x]` Done
- `[ ]` Pending
- `[~]` Partly done / needs improvement

## Today Tasks - 2026-04-29

- `[x]` Create persistent audit checklist for `id-card-edit-lab`.
- `[x]` Add unsaved-changes tracking after design edits.
- `[x]` Add warning before changing institute/batch, reset, template load, refresh, or leaving page with unsaved changes.
- `[x]` Add visible design status: loaded source, unsaved, saving, saved, failed.
- `[x]` Confirm destructive actions: reset layout, remove core field, apply template over current design.
- `[x]` Escape custom layer names in layer list rendering.
- `[x]` Reduce first-screen toolbar clutter so the canvas is visible sooner.
- `[x]` Update this checklist after each completed change.

## Current Page Audit

- `[x]` Page opens at `/id-card-edit-lab`.
- `[x]` Admin protection is active for the page.
- `[x]` Main JavaScript passes syntax check.
- `[x]` Initial browser load shows no console errors.
- `[x]` Front and back side editing exists.
- `[x]` Landscape and portrait orientation exists.
- `[x]` Batch design save API exists.
- `[x]` Institute design save API exists.
- `[x]` Template save/load exists.
- `[x]` Background library tools exist.
- `[x]` Basic PNG/PDF export exists for the current canvas side.

## Must Fix First

- `[x]` Add unsaved-changes tracking after any design edit.
- `[x]` Warn before leaving, changing institute, changing batch, reset, or template load when unsaved changes exist.
- `[x]` Show clear loaded source: no design, institute design, or batch design.
- `[x]` Show clear save state: saved, saving, failed, unsaved changes.
- `[x]` Confirm destructive actions: reset layout, remove core field, apply template over current design.
- `[x]` Escape custom layer names in layer list rendering.
- `[~]` Make top toolbar smaller so canvas is visible on first screen.
- `[ ]` Move advanced controls into grouped menus or side-panel tabs.

## Production Functions Missing

- `[ ]` Export front side only.
- `[ ]` Export back side only.
- `[ ]` Export front and back together as one PDF.
- `[ ]` Batch export all cards for selected batch.
- `[ ]` Batch print front/back for selected batch.
- `[ ]` Preview final card before export.
- `[ ]` Print-size validation before export.
- `[ ]` Warn if objects are outside the card boundary.
- `[ ]` Warn if photo is missing or cannot load.
- `[ ]` Warn if background does not cover the full card.
- `[ ]` Copy current batch design to another batch.
- `[ ]` Apply institute design to current batch.
- `[ ]` Rename saved template.
- `[ ]` Delete saved template.
- `[ ]` Duplicate saved template.

## Editor Quality Improvements

- `[ ]` Convert side panel to tabs: Properties, Layers, Data, Background.
- `[x]` Keep only common controls visible in top bar.
- `[ ]` Add grid toggle.
- `[ ]` Add snap toggle.
- `[ ]` Add center-on-card action.
- `[ ]` Add align top, bottom, vertical center, horizontal center.
- `[ ]` Add distribute evenly.
- `[ ]` Add match width and match height.
- `[ ]` Add lock all fixed fields.
- `[ ]` Add lock background.
- `[ ]` Add object boundary clamp option.
- `[ ]` Add zoom-to-fit on load.
- `[ ]` Improve empty state when no institute, batch, or card is selected.

## Data And Safety

- `[ ]` Make save buttons disabled until required institute/batch exists.
- `[ ]` Prevent batch save when no batch records are loaded, unless user confirms saving a blank batch design.
- `[ ]` Show selected institute and batch in the footer/status area.
- `[ ]` Show last saved time for batch design.
- `[ ]` Show last saved time for institute design.
- `[ ]` Add better error messages for missing records.
- `[ ]` Add better error messages for Supabase/local data mode.
- `[ ]` Validate background URL before saving linked background.
- `[ ]` Validate uploaded background type and size.
- `[ ]` Add recovery if photo/background fails because of CORS.

## Responsive And Visual Checks

- `[ ]` Check desktop 1366px width.
- `[ ]` Check desktop 1920px width.
- `[ ]` Check laptop/small screen around 1024px width.
- `[ ]` Check mobile/tablet behavior or intentionally mark unsupported.
- `[ ]` Ensure top toolbar text does not wrap awkwardly.
- `[ ]` Ensure right panel is visible and usable.
- `[ ]` Ensure canvas is visible without horizontal scrolling on normal laptop screens.
- `[ ]` Ensure footer status text does not overflow.

## Verification To Run After Changes

- `[ ]` Open `/id-card-edit-lab` in browser.
- `[ ]` Confirm no console errors.
- `[ ]` Select institute.
- `[ ]` Select batch.
- `[ ]` Load first card.
- `[ ]` Move a field and confirm unsaved state appears.
- `[ ]` Save batch design and confirm saved state appears.
- `[ ]` Switch front/back and confirm both sides render.
- `[ ]` Export PNG.
- `[ ]` Export PDF.
- `[ ]` Reload page and confirm saved design restores.
