import {
  readdirSync,
  statSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
  copyFileSync,
} from "fs";
import { join, dirname } from "path";

function buildDir(path: string, readPrefix: string, writePrefix: string) {
  for (const item of readdirSync(path)) {
    const itemPath = join(path, item);
    const itemStat = statSync(itemPath);

    if (itemStat.isDirectory()) {
      buildDir(itemPath, readPrefix, writePrefix);
    } else if (itemStat.isFile()) {
      const writePath = join(writePrefix, itemPath.slice(readPrefix.length - 1));
      mkdirSync(dirname(writePath), { recursive: true });

      if (item.endsWith(".ts")) {
        const content = readFileSync(itemPath, "utf8");
        const lines = content.split("\n");

        for (let i = 0; i < lines.length; i++) {
          const trimmed = lines[i].trim();
          if (trimmed.startsWith("import") && trimmed.endsWith('.js"')) {
            lines[i] = `${trimmed.slice(0, -4)}.ts"`;
          }
        }

        writeFileSync(writePath, lines.join("\n"));
      } else {
        copyFileSync(itemPath, writePath);
      }
    }
  }
}

buildDir("./src", "./src", "./dist/deno");
