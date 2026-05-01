const fs = require('fs');
const filepath = 'frontend/src/features/dossie/components/DossieContatoDetalhe.test.tsx';
let content = fs.readFileSync(filepath, 'utf8');

if (!content.includes('import { fireEvent }')) {
  content = content.replace(
    'import { render, screen } from "@testing-library/react";',
    'import { render, screen, fireEvent } from "@testing-library/react";'
  );
}

fs.writeFileSync(filepath, content);
