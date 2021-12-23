#/usr/bin/env python3

from Cheetah.Template import Template
import argparse
import sys
import yaml

help_epilog="""
The error_codes_spec YAML document is a mapping containing two toplevel fields:
    `error_categories`: sequence of string - The error category names
    `error_codes`: sequence of map - Each map consists of:
          `code`: scalar - error's integer value
          `name`: scalar - error's string name
          `extra`: (optional) scalar - C++ class name for holding ErrorExtraInfo.
          `categories`: (optional) sequence of strings - each must appear in `error_categories`.
          `extraIsOptional': (optional) boolean - determines if ErrorExtraInfo can be optional for
                            the ErrorCode.
"""

def init_parser():
    global parser
    parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=__doc__,
            epilog=help_epilog)
    parser.add_argument('--verbose',
            action='store_true',
            help='extra debug logging to stderr')
    parser.add_argument('error_codes_spec',
            help='YAML file describing error codes and categories')
    parser.add_argument('template_file',
            help='Cheetah template file')
    parser.add_argument('output_file')

verbose = False

class ErrorCode:
    def __init__(self, name, code, extra=None, extraIsOptional=False):
        self.name = name
        self.code = code
        self.extra = extra
        self.extraIsOptional = extraIsOptional
        if extra:
            split = extra.split('::')
            if not split[0]:
                raise Exception(
                        f"Error for {name} with extra info {extra}: fully qualified namespaces aren't supported")
            if split[0] == "realm":
                raise Exception(f"Error for {name} with extra info {extra}: don't include the realm namespace")
            if len(split) > 1:
                self.extra_class = split.pop()
                self.extra_ns = "::".join(split)
            else:
                self.extra_class = extra
                self.extra_ns = None
        self.categories = []

class ErrorClass:
    def __init__(self, name, codes):
        self.name = name
        self.codes = codes

def parse_error_definitions_from_file(errors_filename):
    error_codes = []
    error_classes = []
    with open(errors_filename, 'r') as errors_file:
        doc = yaml.safe_load(errors_file)

    if verbose:
        yaml.dump(doc, sys.stderr)

    cats = {}
    for v in doc['error_categories']:
        cats[v] = []

    for v in doc['error_codes']:
        assert type(v) is dict
        name, code = v['name'], v['code']
        extraIsOptional = False

        if 'extraIsOptional' in v:
            extraIsOptional = v['extraIsOptional']

        if 'categories' in v:
            for cat in v['categories']:
                assert cat in cats, f'invalid category {cat} for code {name}'
                cats[cat].append(name)

        kw = {}
        if 'extra' in v:
            kw['extra'] = v['extra']

        error_codes.append(ErrorCode(name, code, **kw, extraIsOptional=extraIsOptional))

    for cat, members in cats.items():
        error_classes.append(ErrorClass(cat, members))

    error_codes.sort(key=lambda x: x.code)

    return error_codes, error_classes

def render_template(template_path, **kw):
    '''Renders the template file located at template_path, using the variables defined by kw, and
       returns the result as a string'''

    template = Template.compile(
        file=template_path,
        compilerSettings=dict(directiveStartToken="//#", directiveEndToken="//#",
                              commentStartToken="//##"), baseclass=dict, useCache=False)
    return str(template(**kw))

def check_for_conflicts(error_codes, error_classes):
    failed = has_duplicate_error_codes(error_codes)
    if has_duplicate_error_classes(error_classes):
        failed = True
    if has_missing_error_codes(error_codes, error_classes):
        failed = True
    if failed:
        raise Exception("Conflicts detected in error code definitions")

def has_duplicate_error_codes(error_codes):
    sorted_by_name = sorted(error_codes, key=lambda x: x.name)
    sorted_by_code = sorted(error_codes, key=lambda x: x.code)

    failed = False
    prev = sorted_by_name[0]
    for curr in sorted_by_name[1:]:
        if curr.name == prev.name:
            sys.stdout.write(f'Duplicate name {curr.name} with codes {curr.code} and {prev.code}\n')
            failed = True
        prev = curr

    prev = sorted_by_code[0]
    for curr in sorted_by_code[1:]:
        if curr.code == prev.code:
            sys.stdout.write(f'Duplicate code {curr.name} with codes {curr.code} and {prev.code}\n')
            failed = True
        prev = curr

    return failed

def has_duplicate_error_classes(error_classes):
    names = sorted(ec.name for ec in error_classes)

    failed = False
    prev_name = names[0]
    for name in names[1:]:
        if prev_name == name:
            sys.stdout.write(f'Duplicate error class name {name}\n')
            failed = True
        prev_name = name
    return failed

def has_missing_error_codes(error_codes, error_classes):
    code_names = dict((ec.name, ec) for ec in error_codes)
    failed = False
    for category in error_classes:
        for name in category.codes:
            try:
                code_names[name].categories.append(category.name)
            except KeyError:
                sys.stdout.write(f'Undeclared error code {name} in class {category.name}\n')
                failed = True

    return failed

def main():
    init_parser()
    parsed = parser.parse_args()
    global verbose
    verbose = parsed.verbose
    error_codes_spec = parsed.error_codes_spec
    template_file = parsed.template_file
    output_file = parsed.output_file

    # Parse and validate error_codes.yml
    error_codes, error_classes = parse_error_definitions_from_file(error_codes_spec)
    check_for_conflicts(error_codes, error_classes)

    # Render the templates to the output files.
    if verbose:
        print(f'rendering {template_file} => {output_file}')
    text = render_template(template_file,
            codes=error_codes,
            categories=error_classes,
            )
    with open(output_file, 'w') as outfile:
        outfile.write(text)

if __name__ == '__main__':
    main()
