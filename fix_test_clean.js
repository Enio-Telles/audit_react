const fs = require('fs');
const filepath = 'frontend/src/features/dossie/components/DossieContatoDetalhe.test.tsx';
let content = fs.readFileSync(filepath, 'utf8');

if (!content.includes('import { fireEvent')) {
  content = content.replace('import { render, screen }', 'import { render, screen, fireEvent }');
}

// Remove duplicated import if any
content = content.replace('import { fireEvent } from "@testing-library/react";\n    ', '');

fs.writeFileSync(filepath, content);
