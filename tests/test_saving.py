#
#  PyCurator LGPL 3.0 <https://www.gnu.org/licenses/lgpl-3.0.txt>
#  Copyright (c) 2022. Michael Baluja
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

from pycurator import utils


def test_openpyxl_save_options():
    try:
        import openpyxl
    except ImportError:
        assert 'Excel' not in utils.save_options
    else:
        assert 'Excel' in utils.save_options


def test_pyarrow_save_options():
    try:
        import pyarrow
    except ImportError:
        assert 'Parquet' not in utils.save_options \
               and 'Feather' not in utils.save_options
    else:
        assert 'Parquet' in utils.save_options \
               and 'Feather' in utils.save_options
