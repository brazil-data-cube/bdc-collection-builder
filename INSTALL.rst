..
    This file is part of Python Module for BDC Collection Builder.
    Copyright (C) 2019 INPE.

    BDC Collection Builder is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.


Installation
============

``bdc-collection-builder`` has essentially several dependencies. Please, read the instructions below in order to install ``collection-builder``.


.. note::

    The ``bdc-collection-builder`` requires `GDAL 2+` to work properly. Make sure that ``gdal-config`` is installed on PATH.


Production installation
-----------------------

**Under Development!**

.. Install from `PyPI <https://pypi.org/>`_:
..
.. .. code-block:: shell
..
..     $ pip3 install bdc-collection-builder


Development installation
------------------------

Clone the software repository:

.. code-block:: shell

        $ git clone https://github.com/brazil-data-cube/bdc-collection-builder.git


Go to the source code folder:

.. code-block:: shell

        $ cd bdc-collection-builder


Install in development mode:

.. code-block:: shell

        $ pip3 install -e .[all]


Generate the documentation:

.. code-block:: shell

        $ python setup.py build_sphinx


The above command will generate the documentation in HTML and it will place it under:

.. code-block:: shell

    doc/sphinx/_build/html/
