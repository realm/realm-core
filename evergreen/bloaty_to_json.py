#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from csv import DictReader
from pathlib import Path

parser = argparse.ArgumentParser(description='Checks how bloated realm has become')
parser.add_argument(
    '--short-symbols-input',
    type=Path,
    help='Path to CSV output of short symbols input file',
)
parser.add_argument(
    '--sections-input',
    type=Path,
    help='Path to CSV output of sections input file',
)

parser.add_argument(
    '--compileunits-input',
    type=Path,
    help='Path to CSV output of compileunits input file',
)

parser.add_argument(
    '--analyzed-file',
    type=str,
    help='Name of file being analyzed by bloaty',
)

evgOpts = parser.add_argument_group('Evergreen Metadata')
evgOpts.add_argument('--output', type=Path, help='The evergreen json output filename')
evgOpts.add_argument('--project', type=str, help='Evergreen project this script is running in')
evgOpts.add_argument('--execution', type=int, help='Execution # of this evergreen task')
evgOpts.add_argument(
    '--is-patch',
    type=bool,
    dest='is_patch',
    help='Specify if this is not a patch build',
)
evgOpts.add_argument(
    '--build-variant',
    type=str,
    dest='build_variant',
    help='Build variant of the evergreen task',
)
evgOpts.add_argument('--branch', type=str, help='Git branch that was being tested')
evgOpts.add_argument('--revision', type=str, help='Git sha being tested')
evgOpts.add_argument('--task-id', type=str, dest='task_id', help='Evergreen task ID of this task')
evgOpts.add_argument('--task-name', type=str, dest='task_name', help='Name of this evergreen task')
evgOpts.add_argument(
    '--revision-order-id',
    type=str,
    dest='revision_order_id',
    help='Evergreen revision order id',
)
evgOpts.add_argument('--version-id', type=str, dest='version_id', help='Name of this evergreen version')

args = parser.parse_args()
patch_username : str = ''

def parse_patch_order():
    global patch_username
    patch_order_re = re.compile(r"(?P<patch_username>[\w\@\.]+)_(?P<patch_order>\d+)")
    match_obj = patch_order_re.match(args.revision_order_id)
    patch_username = match_obj.group('patch_username')
    return int(match_obj.group('patch_order'))
evg_order = int(args.revision_order_id) if not args.is_patch else parse_patch_order()

cxx_method_re = re.compile(
        # namespaces/parent class name
        r"(?P<ns>(?:(?:[_a-zA-Z][\w]*)(?:<.*>)?(?:::)|(?:\(anonymous namespace\)::))+)" +
        r"(?P<name>[\~a-zA-Z_][\w]*)(?:<.*>)?" + # function/class name
        r"(?P<is_function>\(\))?" + # if this is function, this will capture "()"
        # will be a number if this is a lambda
        r"(?:::\{lambda\(\)\#(?P<lambda_number>\d+)\}::)?")

elf_section_re = re.compile(r"\[section \.(?P<section_name>[\w\.\-]+)\]")

items : list[dict] = []
sections_seen = set()
if args.short_symbols_input:
    with open(args.short_symbols_input, 'r') as csv_file:
        input_csv_reader = DictReader(csv_file)
        for row in input_csv_reader:
            raw_name = row['shortsymbols']
            if match := cxx_method_re.search(raw_name):
                ns = match.group('ns').rstrip(':')

                node_name = match.group('name')
                if match.group('lambda_number'):
                    node_name = "{} lambda #{}".format(node_name, match.group('lambda_number'))

                type_str: str = 'symbol'
                if match.group('lambda_number'):
                   type_str = 'lambda'
                elif match.group('is_function'):
                   type_str = 'function'

                items.append({
                    'type': type_str,
                    'name': raw_name,
                    'ns': ns,
                    'file_size': int(row['filesize']),
                    'vm_size': int(row['vmsize']),
                })

            elif match := elf_section_re.search(raw_name):
                section_name = match.group('section_name')
                type_str: str = 'section' if not section_name.startswith('.debug') else 'debug_section'
                if section_name not in sections_seen:
                    items.append({
                        'type': type_str,
                        'name': section_name,
                        'file_size': int(row['filesize']),
                        'vm_size': int(row['vmsize'])
                    })
            else:
                items.append({
                    'type': 'symbol',
                    'name': raw_name,
                    'file_size': int(row['filesize']),
                    'vm_size': int(row['vmsize']),
                })

if args.sections_input:
    with open(args.sections_input, 'r') as csv_file:
        input_csv_reader = DictReader(csv_file)

        for row in input_csv_reader:
            section_name = row['sections']
            type_str: str = 'section' if not section_name.startswith('.debug') else 'debug_section'
            if section_name not in sections_seen:
                items.append({
                    'name': section_name,
                    'type': type_str,
                    'file_size': int(row['filesize']),
                    'vm_size': int(row['vmsize'])
                })

if args.sections_input:
    with open(args.compileunits_input, 'r') as csv_file:
        input_csv_reader = DictReader(csv_file)

        for row in input_csv_reader:
            compileunit_name = row['compileunits']
            if not elf_section_re.search(compileunit_name):
                items.append({
                    'name': compileunit_name,
                    'type': 'compileunit',
                    'file_size': int(row['filesize']),
                    'vm_size': int(row['vmsize'])
                })

output_obj = {
    'items': items,
    'execution': args.execution,
    'is_mainline': (args.is_patch is not True),
    'analyzed_file': args.analyzed_file,
    'order': evg_order,
    'project': args.project,
    'branch': args.branch,
    'build_variant': args.build_variant,
    'revision': args.revision,
    'task_id': args.task_id,
    'task_name': args.task_name,
    'version_id': args.version_id,
    'patch_username': patch_username
}

with open(args.output, 'w') as out_fp:
    json.dump(output_obj, out_fp)
