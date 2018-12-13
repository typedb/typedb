#!/usr/bin/env python

import sys
import re


def parse_deployment_properties(fn):
    deployment_properties = {}
    with open(fn) as deployment_properties_file:
        for line in deployment_properties_file.readlines():
            if line.startswith('#'):
                # skip comments
                pass
            elif '=' in line:
                k, v = line.split('=')
                deployment_properties[k] = v.strip()
    return deployment_properties


_, rules_template_fn, rules_output, deployment_properties_fn, version_file_lbl, deployment_properties_lbl = sys.argv


with open(rules_template_fn) as rules_template_file:
    rules_template = rules_template_file.read()

properties = parse_deployment_properties(deployment_properties_fn)

SUBSTITUTIONS = {
    '{version_file_placeholder}': version_file_lbl,
    '{deployment_properties_placeholder}': deployment_properties_lbl,
    '{maven_packages}': properties['maven.packages']
}

for original, substitution in SUBSTITUTIONS.items():
    rules_template = re.sub(original, substitution, rules_template)

with open(rules_output, 'w') as rules_bzl:
    rules_bzl.write(rules_template)
