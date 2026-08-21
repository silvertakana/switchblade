# Local Model Router - Visual Polish Plan

## Screenshots Analyzed
- Playground tab: Chat interface with reasoning display, debug panel
- Dashboard tab: Backend cards, traffic stats, request history, models table, config viewer

## Critical Visual Issues to Fix

### 1. Raw Markdown in Reasoning Display
**Issue:** The `**bold text**` markdown in reasoning is displayed literally instead of being rendered as bold HTML.
**Location:** `renderThread()` function, reasoning message rendering
**Fix:** Parse simple markdown (bold, italic) before rendering. Use a lightweight regex to convert `**text**` to `<strong>text</strong>`.

### 2. Raw JSON Error Messages in Backend Cards
**Issue:** The escaped JSON strings in backend cards look messy and unprofessional. Example: `{"status":429,"kind":"weekly","body":"{\"type\":\"error\",\"error...`
**Location:** `renderBackends()` function, errShort display
**Fix:** Show a cleaner, human-readable error summary instead of raw JSON. Keep the full JSON in the expandable errline, but show a truncated, cleaner version by default.

### 3. Inconsistent Border Radius
**Issue:** Buttons use 6px, cards use 8px, pre blocks use 8px. Should be consistent.
**Location:** CSS variables and component styles
**Fix:** Standardize to 8px for all interactive elements and containers. Use `--radius: 8px` variable.

### 4. "Advanced" Link Styling
**Issue:** Currently just plain text with underline. Should be more visible as a button or styled link.
**Location:** Playground tab, Advanced toggle
**Fix:** Style as a subtle button or pill with hover state, not just underlined text.

### 5. Debug Cell Spacing
**Issue:** The four debug cards could be more evenly distributed.
**Location:** `.debug-grid` CSS
**Fix:** Ensure equal spacing and consistent card heights.

### 6. Traffic Stats Bar Visual Hierarchy
**Issue:** The horizontal bar could have better visual hierarchy with labels inside or above.
**Location:** `renderStats()` function
**Fix:** Add percentage label inside the bar or make the bar more prominent.

### 7. Card Padding Consistency
**Issue:** Backend cards have slightly different padding.
**Location:** `.card` CSS
**Fix:** Ensure consistent padding across all card types.

### 8. Scroll Bar Styling
**Issue:** The custom scrollbar could be styled to match the theme.
**Location:** CSS
**Fix:** Add custom scrollbar styles using `::-webkit-scrollbar` and `scrollbar-width`.

### 9. User Message Bubble
**Issue:** The white background is good but could have slightly more padding.
**Location:** `.msg.user` CSS
**Fix:** Add slightly more padding for better readability.

### 10. Assistant Message Bubble
**Issue:** The yellow left border is good but could be more subtle.
**Location:** `.msg.assistant` CSS
**Fix:** Make the border slightly thinner or use a more subtle color.

### 11. Typography Hierarchy
**Issue:** Some headings could be more refined.
**Location:** Various heading styles
**Fix:** Ensure consistent font weights and sizes across sections.

### 12. Button Hover States
**Issue:** Buttons have hover states but could be more polished.
**Location:** Button CSS
**Fix:** Add smoother transitions and more refined hover effects.

### 13. Focus States
**Issue:** Focus states not visible in screenshots but should be checked.
**Location:** CSS
**Fix:** Ensure visible focus states for accessibility.

### 14. Responsive Design
**Issue:** Need to check how it looks on smaller screens.
**Location:** Media queries
**Fix:** Ensure the UI adapts well to different screen sizes.

### 15. Config Viewer Syntax Highlighting
**Issue:** The JSON display could be syntax-highlighted.
**Location:** Config viewer
**Fix:** Add basic syntax highlighting for JSON (keys, strings, numbers, booleans).

## Implementation Order
1. Fix critical visual issues (1-4)
2. Improve spacing and consistency (5-8)
3. Polish interactions and details (9-12)
4. Add finishing touches (13-15)

## Verification
After implementation, take new screenshots and compare with the original to ensure all issues are fixed and no new issues are introduced.
