import { readFileSync } from 'node:fs'

/** Read NAME or NAME_FILE, never both. File contents are trimmed at startup. */
export function readExclusiveInput(
  env: Readonly<Record<string, string | undefined>>,
  name: string,
  required = false,
): string | undefined {
  const direct = env[name]
  const file = env[`${name}_FILE`]
  if (direct !== undefined && file !== undefined) throw new Error(`${name} and ${name}_FILE are mutually exclusive`)
  let value = direct
  if (file !== undefined) {
    if (!file) throw new Error(`${name}_FILE must be a non-empty path`)
    try { value = readFileSync(file, 'utf8').trim() } catch { throw new Error(`${name}_FILE could not be read`) }
  }
  if (required && !value) throw new Error(`${name} is required`)
  return value || undefined
}
