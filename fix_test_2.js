const fs = require('fs');
const filepath = 'frontend/src/features/dossie/components/DossieContatoDetalhe.test.tsx';
let content = fs.readFileSync(filepath, 'utf8');

// Need to click tabs to reveal content inside them
content = content.replace(
  'expect(screen.getByText("Contador Consolidado")).toBeInTheDocument();',
  'import { fireEvent } from "@testing-library/react";\n    fireEvent.click(screen.getByText("Contadores"));\n    expect(screen.getByText("Contador Consolidado")).toBeInTheDocument();'
);

content = content.replace(
  'expect(screen.getByText("Socio Atual")).toBeInTheDocument();',
  'fireEvent.click(screen.getByText("Sócios"));\n    expect(screen.getByText("Socio Atual")).toBeInTheDocument();'
);

// We need to move the fireEvent import to the top or just use `act` + `fireEvent`
// Better yet, write a script to fix it cleanly
fs.writeFileSync(filepath, content);
