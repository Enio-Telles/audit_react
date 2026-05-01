// simple test just to structure the single-pass loop
const entidades = []; // dummy

let totalTelefones = 0;
let totalEmails = 0;
let totalEnderecos = 0;
let totalConflitos = 0;
let totalSemContato = 0;

for (const entidade of entidades) {
  totalTelefones += entidade.telefones.length;
  totalEmails += entidade.emails.length;
  totalEnderecos += entidade.enderecos.length;

  const status = calcular_status_entidade(entidade);
  if (status === "divergente") {
    totalConflitos++;
  } else if (status === "sem contato") {
    totalSemContato++;
  }
}
