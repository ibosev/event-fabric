/** Dependencies must be caught up before an offline rebuild can read them. */
export function readmodelOrder(
  groups: Record<string, { reads?: string[] }>,
): string[] {
  const ordered: string[] = [],
    visited = new Set<string>();
  const visit = (name: string, path: string[] = []) => {
    if (path.includes(name))
      throw new Error(
        `Readmodel dependency cycle: ${[...path, name].join(" -> ")}`,
      );
    if (!Object.hasOwn(groups, name))
      throw new Error(`Unknown declared Readmodel group ${name}`);
    if (visited.has(name)) return;
    for (const dependency of groups[name]!.reads ?? [])
      visit(dependency, [...path, name]);
    visited.add(name);
    ordered.push(name);
  };
  for (const name of Object.keys(groups)) visit(name);
  return ordered;
}
