from pathlib import Path
base = Path('/var/www/cnpj')
user_input1 = '/etc/passwd'
user_input2 = '../../etc/passwd'
user_input3 = '/var/www/cnpj/../../etc/passwd'

print("Base:", base)
for p in [user_input1, user_input2, user_input3]:
    resolved = Path(p).resolve()
    print(f"Input: {p} -> Resolved: {resolved} -> Is relative: {resolved.is_relative_to(base)}")

    # Memory approach
    if '..' in p:
        print(f"Input: {p} -> Rejected by .. check")
    else:
        full = base / p if not Path(p).is_absolute() else Path(p)
        print(f"Input: {p} -> Full: {full.resolve()} -> Is relative: {full.resolve().is_relative_to(base)}")
