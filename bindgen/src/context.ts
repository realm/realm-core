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

import { BoundSpec } from "./bound-model";
import { Formatter } from "./formatter";
import { Outputter } from "./outputter";
import { Spec } from "./spec";

export type TemplateContext = {
  rawSpec: Spec;
  spec: BoundSpec;
  /**
   * @param path The file path, relative to the output directory.
   * @param formatter An optional formatter to run after the template has returned.
   * The invocation is batched, such that a single formatter is only ever executed once but passed all file paths which use
   * @returns An outputter, which can be used to write to the file.
   */
  file: (path: string, formatter?: Formatter) => Outputter;
};
