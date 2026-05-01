const fs = require('fs');
const filepath = 'frontend/src/features/dossie/components/DossieContatoDetalhe.test.tsx';
let content = fs.readFileSync(filepath, 'utf8');
content = content.replace(/Agenda dos contadores/g, 'Contadores');
content = content.replace(/Agenda da empresa/g, 'Empresa');
content = content.replace(/Agenda dos socios/g, 'Sócios');
fs.writeFileSync(filepath, content);
