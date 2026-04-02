## 2024-05-24 - [Header Context Input Accessibility]
**Learning:** Header inputs that act as global context filters (like the CNPJ input) often use `<span>` for labels instead of proper `<label htmlFor="...">` tags, reducing click targets and screen reader context.
**Action:** Always wrap text adjacent to inputs in `<label>` tags with `htmlFor` and add `cursor-pointer` to indicate interactivity.