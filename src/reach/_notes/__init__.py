"""Notebook-only REACH helpers grouped by concern.

``reach._notes`` sits beside ``reach._expressions`` and ``reach._transforms``.
It carries workbook state, display helpers, and notebook assembly code that are
essential to notebooks but are not pure Spark transform logic. Import concrete
concerns such as ``reach._notes._clinical_oversight`` or
``reach._notes._copay_waiver`` directly.
"""
