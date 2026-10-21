import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const root = path.dirname(fileURLToPath(import.meta.url));

/** The desktop screens import this font. On Vercel the file lives outside `web/`, so point at whichever node_modules has the package. */
function fontsource() {
  const name = '@fontsource-variable/inter';
  const candidates = [path.join(root, 'node_modules', name), path.join(root, '..', 'node_modules', name)];
  const dir = candidates.find((item) => fs.existsSync(path.join(item, 'package.json'))) ?? candidates[0];
  return path.join(dir, 'index.css');
}

export default defineConfig({
  server: { port: 5174, strictPort: true, fs: { allow: [path.resolve(root, '..')] } },
  preview: { port: 5174, strictPort: true },
  resolve: { alias: { '@desklist/shared': path.join(root, '../shared/src/index.ts') } },
  define: { __APP_VERSION__: JSON.stringify(JSON.parse(fs.readFileSync(path.join(root, '../app/package.json'), 'utf8')).version) },
  plugins: [react()],
});
