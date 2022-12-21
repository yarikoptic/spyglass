import collections
import datajoint as dj
import numpy as np
import platform
import re
import subprocess
import spyglass
import warnings


class ProvMixinError(dj.DataJointError):
    pass


class ProvMixinWarning(UserWarning):
    pass


def is_spyglass_editable():
    """Return True if spyglass is installed in editable mode (i.e., `pip install -e spyglass`).

    Spyglass is currently installed only via pip from PyPI or directly from GitHub.
    """
    pip_list_out = subprocess.run(["pip", "list"], capture_output=True)
    matches = re.search(
        "spyglass-neuro\s+(.*)\s+(.*spyglass)", str(pip_list_out.stdout)
    )
    return matches is not None


def 


class ProvMixin:
    # secondary attributes to add to a table definition using
    # `definition = """...""" + ProvMixin.added_definition`
    added_definition = """
    spyglass_version: varchar(100)              # version of spyglass used to modify the entry
    analysis_code_version: varchar(100)         # version of local software used to modify the entry
    modified_by: varchar(100)                   # datajoint user who modified the entry
    modified_at = CURRENT_TIMESTAMP: timestamp  # time when entry was last modified
    modified_on = "": varchar(100)              # hostname of system used to modify the entry
    """

    # keep these in sync with `added_definition`
    added_attributes = [
        "spyglass_version",
        "analysis_code_version",
        "modified_by",
        "modified_at",
        "modified_on",
    ]

    # override Table.insert
    def insert(
        self,
        rows,
        replace=False,
        skip_duplicates=False,
        ignore_extra_fields=False,
        allow_direct_insert=None,
    ):
        new_rows = list()
        for row in rows:
            new_row = None
            if isinstance(row, np.void):  # numpy structured array
                for added_attr in self.added_attributes:
                    if added_attr in row.dtype.fields:
                        raise ProvMixinError(
                            f"Invalid insert argument. ProvMixin attribute '{added_attr}' cannot be specified."
                        )
                new_row = {name: row[name] for name in row.dtype.fields}
            elif isinstance(row, collections.abc.Mapping):  # dict-based
                for added_attr in self.added_attributes:
                    if added_attr in row:
                        raise ProvMixinError(
                            f"Invalid insert argument. ProvMixin attribute '{added_attr}' cannot be specified."
                        )
                new_row = row.copy()  # NOTE this is a shallow copy
            else:  # positional
                if len(row) != (len(self.heading) - len(self.added_attributes)):
                    raise ProvMixinError(
                        f"Invalid insert argument. Incorrect number of attributes: "
                        f"{len(row)} given; {len(self.heading) - len(self.added_attributes)} expected."
                    )

                # convert to dictionary
                attrs_without_added = [
                    h for h in self.heading if h not in self.added_attributes
                ]
                new_row = {name: value for name, value in zip(attrs_without_added, row)}

            if is_spyglass_editable():
                warnings.warn(
                    "Spyglass is installed in editable mode.", category=ProvMixinWarning
                )

            new_row["spyglass_version"] = spyglass.__version__
            new_row["modified_by"] = self.connection.get_user()
            new_row["modified_on"] = platform.uname().node
            # use default value for modified_at

            new_rows.append(new_row)

        super().insert(
            new_rows,
            replace,
            skip_duplicates,
            ignore_extra_fields,
            allow_direct_insert,
        )
