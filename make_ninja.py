#!/usr/bin/env python3

import ninja_syntax
import yaml
from yaml import CSafeLoader as LOADER
#from yaml import SafeLoader as LOADER
from pprint import pprint
import os
import sys
import io

def parse_makefile(path):
    # TODO less crappy parser
    with open(path) as f:
        text = f.read()
    text = text.replace('\\\n', '')
    target, source = text.split(':', 1)
    return (
        target.strip(),
        [s.strip() for s in source.split()],
    )

def flatten(in_list, prefix=''):
    assert(isinstance(in_list, list))
    out_list = []
    for item in in_list:
        if isinstance(item, dict):
            for path, sub_list in item.items():
                combined = os.path.join(prefix, path)
                out_list.extend(flatten(sub_list, combined))
        else:
            out_list.append(os.path.join(prefix, item))
    return out_list

with open('make_ninja.yml') as f:
    config = yaml.load(f, Loader=LOADER)

header_units = flatten(config['header_units'])
libs = config['libs']
bins = config['bins']

def to_out_base(path):
    return os.path.join('build_mod', path.replace(os.sep, '_'))

def write_if_changed(s, path, *, force=False):
    if not force and os.path.exists(path):
        with open(path) as f:
            if f.read() == s:
                return

    with open(path, 'w') as f:
        f.write(s)

def make_ninja():
    sources = []
    for lib in libs:
        libs[lib]['sources'] = flatten(libs[lib]['sources'])
        sources += libs[lib]['sources']

    for bin in bins:
        bins[bin]['sources'] = flatten(bins[bin]['sources'])
        sources += bins[bin]['sources']

    ninja = ninja_syntax.Writer(io.StringIO())

    objs = []
    scans = []
    pcms = []
    for hu in header_units:
        out_base = to_out_base(hu)

        obj = out_base + '.o'
        pcm = out_base + '.pcm'
        pcm_dyndeps = pcm + '.dd'
        pcm_flags = pcm + '.flags'

        scans.append(pcm_dyndeps)
        pcms.append(pcm)
        objs.append(obj)

        ninja.build(
            pcm_dyndeps, 'SCAN',
            hu,
            implicit=['$CXX', '$MAKE_NINJA', 'make_ninja.yml'],
            #implicit_outputs=pcm_flags,
            variables=dict(
                KIND='c++-header',
                PCM_FILE=pcm,
                PCMFLAGS_FILE=pcm_flags,
            ))
        ninja.build(
            pcm, 'HEADER_UNIT',
            hu,
            order_only=pcm_dyndeps,
            implicit='$CXX',
            #implicit=pcm_flags,
            variables=dict(
                dyndep=pcm_dyndeps,
                PCMFLAGS_FILE=pcm_flags,
            ))
        ninja.build(obj, 'HEADER_UNIT_CXX', pcm, implicit='$CXX')
        ninja.newline()

    for cpp in sources:
        out_base = to_out_base(cpp)
        obj = out_base + '.o'
        obj_flags = obj + '.flags'
        obj_dyndeps = obj + '.dd'

        scans.append(obj_dyndeps)
        objs.append(obj)

        ninja.build(
            obj_dyndeps, 'SCAN',
            cpp,
            implicit=['$CXX', '$MAKE_NINJA', 'make_ninja.yml'],
            #implicit_outputs=pcm_flags,
            variables=dict(
                KIND='c++',
                PCM_FILE=obj,
                PCMFLAGS_FILE=obj_flags,
            ))

        if cpp in config['module_exclusions']:
            obj_flags = '/dev/null'

        ninja.build(
            obj, 'CXX',
            cpp,
            implicit='$CXX',
            order_only=obj_dyndeps,
            variables=dict(
                dyndep=obj_dyndeps,
                FLAGS_FILE=obj_flags,
            ))

    for bin in bins:
        out = to_out_base(bin)
        deps_file = out + '.dd'

        libdeps = bins[bin]['libdeps']
        sources = set(bins[bin]['sources'])
        syslibdeps = set(bins.get('syslibdeps', []))

        def add_libdep_sources(libdeps):
            for libdep in libdeps:
                for s in libs[libdep]['sources']:
                    sources.add(s)
                for l in libs[libdep].get('syslibdeps', ()):
                    syslibdeps.add(l)
                add_libdep_sources(libs[libdep]['libdeps'])
        add_libdep_sources(libdeps)

        ninja.build(
            deps_file, 'LINKSCAN',
            [to_out_base(s)+'.o.dd' for s in sorted(sources)],
            implicit='$MAKE_NINJA',
        )

        variables = {}
        if syslibdeps:
            variables['EXTRALIBS'] = ['-l' + l for l in sorted(syslibdeps)]

        ninja.build(out, 'BIN', deps_file,
                    #implicit=deps_file+'.flags',
                    variables=variables)

    ninja.build('scans', 'phony', scans)
    ninja.build('pcms', 'phony', pcms)
    ninja.build('objs', 'phony', objs)
    ninja.build('bins', 'phony', [to_out_base(b) for b in bins])
    ninja.default('bins')

    with open('builds.inc.ninja', 'w') as f:
        f.write(ninja.output.getvalue())
    with open('build.ninja', 'w') as f:
        f.write('include make_ninja-header.ninja\n')

def scan_header_unit(raw, header_name, dyndeps, pcmflags):
    target, sources = parse_makefile(raw)
    source_mods = []
    source_reals = set()
    hu_reals = {}

    # This is an O(N + M) algorithm for checking os.samefile() on
    # ever file in sources against every file in header_units
    def file_id(path):
        stat = os.stat(path)
        return (stat.st_dev, stat.st_ino)

    self_id = file_id(header_name)
    for s in sources:
        s_id = file_id(s)
        if s_id != self_id: # the input header is listed as a source
            source_reals.add(s_id)

    for hu in header_units:
        hu_reals[file_id(hu)] = to_out_base(hu) + '.pcm'

    for s in source_reals:
        if s in hu_reals:
            source_mods.append(hu_reals[s])

    ninja = ninja_syntax.Writer(io.StringIO())
    ninja.variable('ninja_dyndep_version', 1)
    ninja.build(target, 'dyndep', implicit=source_mods)

    # XXX because the flags file isn't in the ninja deps graph, restat isn't quite safe :(
    write_if_changed(ninja.output.getvalue(), dyndeps)

    write_if_changed(
        ''.join(f'-fmodule-file={s}\n' for s in source_mods),
        pcmflags)

def link_scan(deps_file, *inputs):
    objs = set()
    considered = set(inputs)
    queue = list(inputs)
    while queue:
        input = queue.pop()
        assert(input.endswith('.dd'))
        obj = input[:-3]
        objs.add(obj)
        print('opening:', obj + '.flags')
        with open(obj + '.flags') as f:
            for line in f:
                if not line: continue
                line = line.strip()
                prefix = '-fmodule-file='
                suffix = '.pcm'
                assert(line.startswith(prefix))
                assert(line.endswith(suffix))
                obj = line[len(prefix):-len(suffix)] + '.o'
                objs.add(obj)

                continue

                # This shouldn't be needed. Leaving it so it can easily be enabled to check if it fixes issues.
                dyndep = line[len(prefix):] + '.dd'
                if (dyndep not in considered):
                    considered.add(dyndep)
                    queue.append(dyndep)


    objs = sorted(objs)

    ninja = ninja_syntax.Writer(io.StringIO())
    ninja.variable('ninja_dyndep_version', 1)
    ninja.build(deps_file[:-len('.dd')], 'dyndep', implicit=objs)
    write_if_changed(ninja.output.getvalue(), deps_file, force=True)

    write_if_changed(
        '\n'.join(objs),
        deps_file + '.flags',
        force=True)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        make_ninja()
    elif sys.argv[1] == '--scan':
        scan_header_unit(*sys.argv[2:])
    elif sys.argv[1] == '--link-scan':
        link_scan(*sys.argv[2:])
    else:
        print('Unknown mode: ', sys.argv[1])
