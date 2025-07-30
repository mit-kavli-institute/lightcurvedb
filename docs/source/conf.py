# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../../src"))


# -- Project information -----------------------------------------------------

project = "LightcurveDB"
copyright = "2021, William Fong"
author = "William Fong"

# Import version from package
try:
    from lightcurvedb import __version__

    version = __version__
    release = __version__
except ImportError:
    version = "0.0.0"
    release = "0.0.0"


# -- General configuration ---------------------------------------------------
master_doc = "index"
# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinxcontrib.mermaid",
]

# Suppress warnings about unresolved references in external docstrings
suppress_warnings = ["ref.ref"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]  # Commented out - no custom static files

autodoc_member_order = "bysource"

# Mock imports for documentation build
autodoc_mock_imports = [
    "psycopg",
    "psycopg.adapters",
    "h5py",
    "pandas",
    "scipy",
    "pyquaternion",
    "configurables",
    "pyticdb",
    "pytest",
    "hypothesis",
    "tqdm",
    "cachetools",
    "tabulate",
]

# -- Mermaid configuration ---------------------------------------------------

# Initialize Mermaid with custom theme and settings
mermaid_init_js = """
mermaid.initialize({
  theme: 'neutral',
  themeVariables: {
    fontFamily: 'var(--font-stack)',
    fontSize: '16px',
    primaryColor: '#0066cc',
    primaryTextColor: '#fff',
    primaryBorderColor: '#004499',
    lineColor: '#5c6670',
    secondaryColor: '#e1e4e8',
    tertiaryColor: '#f6f8fa'
  },
  er: {
    entityPadding: 15,
    fontSize: 12
  },
  flowchart: {
    nodeSpacing: 50,
    rankSpacing: 50
  }
});
"""

# Enable zoom functionality for all Mermaid diagrams
mermaid_d3_zoom = True

# Alternative theme configurations (uncomment to use):

# Forest theme - High contrast with green tones
# mermaid_init_js = """
# mermaid.initialize({
#   theme: 'forest',
#   er: {
#     entityPadding: 15,
#     fontSize: 12
#   }
# });
# """

# Dark theme - For dark mode documentation
# mermaid_init_js = """
# mermaid.initialize({
#   theme: 'dark',
#   themeVariables: {
#     fontFamily: 'var(--font-stack)',
#     fontSize: '16px',
#     primaryColor: '#1e90ff',
#     primaryTextColor: '#fff',
#     primaryBorderColor: '#1873cc',
#     lineColor: '#4a5568',
#     secondaryColor: '#2d3748',
#     tertiaryColor: '#1a202c',
#     background: '#0d1117',
#     mainBkg: '#161b22',
#     secondBkg: '#21262d'
#   },
#   er: {
#     entityPadding: 15,
#     fontSize: 12
#   }
# });
# """

# Custom base theme - Matching Furo's exact colors
# mermaid_init_js = """
# mermaid.initialize({
#   theme: 'base',
#   themeVariables: {
#     fontFamily: 'var(--font-stack)',
#     fontSize: '16px',
#     primaryColor: '#007acc',
#     primaryTextColor: '#fff',
#     primaryBorderColor: '#005a9e',
#     lineColor: '#6c757d',
#     secondaryColor: '#f8f9fa',
#     tertiaryColor: '#e9ecef',
#     background: '#ffffff',
#     mainBkg: '#ffffff',
#     secondBkg: '#f8f9fa',
#     nodeBorder: '#dee2e6',
#     clusterBkg: '#f8f9fa',
#     clusterBorder: '#dee2e6',
#     defaultLinkColor: '#6c757d',
#     titleColor: '#212529',
#     edgeLabelBackground: '#ffffff'
#   },
#   er: {
#     entityPadding: 20,
#     fontSize: 12
#   }
# });
# """
