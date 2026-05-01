const fs = require('fs');
const filepath = 'frontend/src/features/dossie/components/DossieContatoDetalhe.test.tsx';
let content = fs.readFileSync(filepath, 'utf8');

if (!content.includes('import { act }')) {
  content = content.replace('import { render, screen } from "@testing-library/react";', 'import { render, screen, fireEvent, act } from "@testing-library/react";');
}

// Ensure the tests click the appropriate tabs.
content = content.replace('expect(screen.getAllByText("Agenda dos contadores").length).toBeGreaterThan(\n      0,\n    );',
  'act(() => {\n      fireEvent.click(screen.getByText("Contadores"));\n    });\n    expect(screen.getAllByText("Contadores").length).toBeGreaterThan(0);'
);

content = content.replace('expect(screen.getAllByText("Agenda dos socios").length).toBeGreaterThan(0);',
  'act(() => {\n      fireEvent.click(screen.getByText("Sócios"));\n    });\n    expect(screen.getAllByText("Sócios").length).toBeGreaterThan(0);'
);

fs.writeFileSync(filepath, content);
