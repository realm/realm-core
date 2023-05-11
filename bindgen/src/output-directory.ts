////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

import fs from "node:fs";
import path from "node:path";
import { strict as assert } from "node:assert";
import chalk from "chalk";
import { Debugger } from "debug";

import { extend } from "./debug";
import { Outputter, createOutputter } from "./outputter";
import { format, Formatter } from "./formatter";

const debug = extend("out");

type OutputFile = {
  fd: number;
  resolvedPath: string;
  debug: Debugger;
  formatter?: Formatter;
};

export type OutputDirectory = {
  /**
   * @param filePath The file path, relative to the output directory.
   * @param formatter An optional formatter to run after the template has returned.
   * The invocation is batched, such that a single formatter is only ever executed once but passed all file paths which use
   * @returns An outputter, which can be used to write to the file.
   */
  file(filePath: string, formatter?: Formatter): Outputter;
  /**
   * Close all files opened during the lifetime of the output directory.
   */
  close(): void;
  /**
   * Formats all files opened during the lifetime of the output directory.
   */
  format(): void;
};

export const usedFormatters = new Set<Formatter>();

/**
 * @param outputPath Path on disk, to the output directory.
 * @returns An object, exposing methods to open files to write into.
 */
export function createOutputDirectory(outputPath: string): OutputDirectory {
  // Create the output directory if it doesn't exist
  if (!fs.existsSync(outputPath)) {
    fs.mkdirSync(outputPath, { recursive: true });
  }
  const openFiles: OutputFile[] = [];
  return {
    file(filePath: string, formatter?: Formatter) {
      const resolvedPath = path.resolve(outputPath, filePath);
      const parentDirectoryPath = path.dirname(resolvedPath);
      if (!fs.existsSync(parentDirectoryPath)) {
        debug("Creating parent directory", parentDirectoryPath);
        fs.mkdirSync(parentDirectoryPath, { recursive: true });
      }
      const fileDebug = debug.extend(filePath);

      if (formatter) {
        assert(formatter.name, "Expected a function with a non-empty name property");
        usedFormatters.add(formatter);
      }

      fileDebug(chalk.dim("Opening", resolvedPath));
      const fd = fs.openSync(resolvedPath, "w");
      openFiles.push({ fd, formatter, resolvedPath, debug: fileDebug });

      return createOutputter((data) => {
        fs.writeFileSync(fd, data, { encoding: "utf8" });
      }, fileDebug);
    },
    close() {
      for (const { fd, debug } of openFiles) {
        debug(chalk.dim("Closing file"));
        fs.closeSync(fd);
      }
    },
    format() {
      for (const formatter of usedFormatters) {
        const relevantFiles = openFiles.filter((file) => file.formatter === formatter).map((f) => f.resolvedPath);
        format(formatter, outputPath, relevantFiles);
      }
    },
  };
}
