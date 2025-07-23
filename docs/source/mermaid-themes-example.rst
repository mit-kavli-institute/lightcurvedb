Mermaid Theme Examples
======================

This page demonstrates different Mermaid theme options for the ER diagram.

Neutral Theme (Default)
-----------------------

.. mermaid::
   :caption: Neutral Theme - Clean and professional
   :align: center

   ---
   config:
     theme: neutral
   ---
   erDiagram
       Mission ||--o{ MissionCatalog : "has"
       MissionCatalog ||--o{ Target : "contains"
       Target ||--o{ DataSet : "has"

       Mission {
           UUID id PK
           string name UK
       }

       MissionCatalog {
           int id PK
           string name UK
       }

       Target {
           bigint id PK
           bigint name
       }

       DataSet {
           int id PK
           array values
       }

Forest Theme
------------

.. mermaid::
   :caption: Forest Theme - High contrast with green tones
   :align: center

   ---
   config:
     theme: forest
   ---
   erDiagram
       Mission ||--o{ MissionCatalog : "has"
       MissionCatalog ||--o{ Target : "contains"
       Target ||--o{ DataSet : "has"

       Mission {
           UUID id PK
           string name UK
       }

       MissionCatalog {
           int id PK
           string name UK
       }

       Target {
           bigint id PK
           bigint name
       }

       DataSet {
           int id PK
           array values
       }

Dark Theme
----------

.. mermaid::
   :caption: Dark Theme - For dark mode documentation
   :align: center

   ---
   config:
     theme: dark
   ---
   erDiagram
       Mission ||--o{ MissionCatalog : "has"
       MissionCatalog ||--o{ Target : "contains"
       Target ||--o{ DataSet : "has"

       Mission {
           UUID id PK
           string name UK
       }

       MissionCatalog {
           int id PK
           string name UK
       }

       Target {
           bigint id PK
           bigint name
       }

       DataSet {
           int id PK
           array values
       }

Default Theme
-------------

.. mermaid::
   :caption: Default Theme - Standard Mermaid appearance
   :align: center

   ---
   config:
     theme: default
   ---
   erDiagram
       Mission ||--o{ MissionCatalog : "has"
       MissionCatalog ||--o{ Target : "contains"
       Target ||--o{ DataSet : "has"

       Mission {
           UUID id PK
           string name UK
       }

       MissionCatalog {
           int id PK
           string name UK
       }

       Target {
           bigint id PK
           bigint name
       }

       DataSet {
           int id PK
           array values
       }

Switching Themes
----------------

To change the theme for all diagrams in your documentation:

1. Edit ``docs/source/conf.py``
2. Find the ``mermaid_init_js`` configuration
3. Uncomment one of the alternative theme configurations
4. Comment out the current configuration
5. Rebuild the documentation

For individual diagrams, you can override the theme in the diagram's config block as shown above.
