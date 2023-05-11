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

import chalk from "chalk";
import cp from "child_process";

import { extend } from "./debug";
const debug = extend("format");

type Formatter = (cwd: string, filePaths: string[]) => void;

export function executeCommand(cwd: string, command: string, ...args: string[]) {
  console.log(chalk.dim(command, ...args));
  const result = cp.spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: "inherit",
    shell: true,
  });
  if (result.error) throw result.error;
  if (result.status) {
    throw new Error(`Exited with status ${result.status}`)
  }
}

function createCommandFormatter(commandAndArgs: string[]): Formatter {
  return (cwd, filePaths) => {
    const [command, ...args] = [...commandAndArgs, ...filePaths];
    executeCommand(cwd, command, ...args);
  };
}

const FORMATTERS: Record<string, Formatter> = {
  eslint: createCommandFormatter(["npx", "eslint", "--fix", "--format=stylish"]),
  "clang-format": createCommandFormatter(["npx", "clang-format", "-i"]),
  // "typescript-checker": ["npx", "tsc", "--noEmit"],
};

export function addFormatter(name: string, callbackOrCommandAndArgs: Formatter | string[]) {
  if (name in FORMATTERS) {
    throw new Error(`Cannot add ${name} as it's already there`);
  } else if (Array.isArray(callbackOrCommandAndArgs)) {
    FORMATTERS[name] = createCommandFormatter(callbackOrCommandAndArgs);
  } else {
    FORMATTERS[name] = callbackOrCommandAndArgs;
  }
}

export function getFormatterNames() {
  return Object.keys(FORMATTERS);
}

export type FormatterName = string;

export class FormatError extends Error {
  constructor(formatterName: string, filePaths: string[], cause: Error) {
    super(`Failure when running the '${formatterName}' formatter on ${JSON.stringify(filePaths)}: ${cause.message}`);
  }
}

export function format(formatterName: FormatterName, cwd: string, filePaths: string[]): void {
  if (filePaths.length === 0) {
    debug(chalk.dim("Skipped running formatter '%s' (no files need it)"), formatterName);
    return;
  } else {
    debug(chalk.dim("Running formatter '%s' on %d files"), formatterName, filePaths.length);
    if (formatterName in FORMATTERS) {
      console.log(`Running '${formatterName}' formatter`);
      try {
        FORMATTERS[formatterName](cwd, filePaths);
      } catch (err) {
        throw new FormatError(formatterName, filePaths, err instanceof Error ? err : new Error(String(err)));
      }
    }
  }
}
