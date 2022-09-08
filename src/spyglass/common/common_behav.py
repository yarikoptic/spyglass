import datajoint as dj
import itertools
import ndx_franklab_novela
import pandas as pd
import pynwb

from .common_ephys import Raw  # noqa: F401
from .common_interval import IntervalList, interval_list_contains
from .common_nwbfile import Nwbfile
from .common_session import Session  # noqa: F401
from .common_task import TaskEpoch
from .dj_helper_fn import fetch_nwb
from .nwb_helper_fn import get_all_spatial_series, get_data_interface, get_nwb_file

schema = dj.schema('common_behav')


@schema
class RawPosition(dj.Imported):
    definition = """Position data that comes from an NWB file
    -> Session
    ---
    -> IntervalList                        # the valid times for this position data
    raw_position_object_id: varchar(40)    # the object id of the spatial series for this epoch in the NWB file
    """

    def make(self, key):
        nwb_file_name = key['nwb_file_name']
        nwb_file_abspath = Nwbfile.get_abs_path(nwb_file_name)
        nwbf = get_nwb_file(nwb_file_abspath)

        # TODO refactor this. this calculates sampling rate (unused here) and is expensive to do twice
        pos_dict = get_all_spatial_series(nwbf)
        if pos_dict is None:
            print(f'Unable to import RawPosition: no SpatialSeries data found in a Position NWB object '
                  f'in {nwb_file_name}.')
            return

        for epoch in pos_dict:
            pdict = pos_dict[epoch]
            pos_interval_list_name = f'pos {epoch} valid times'

            # create the interval list and insert it
            interval_dict = dict()
            interval_dict['nwb_file_name'] = nwb_file_name
            interval_dict['interval_list_name'] = pos_interval_list_name
            interval_dict['valid_times'] = pdict['valid_times']
            IntervalList().insert1(interval_dict, skip_duplicates=True)

            # add this interval list to the table
            key = dict()
            key['nwb_file_name'] = nwb_file_name
            key['interval_list_name'] = pos_interval_list_name
            key['raw_position_object_id'] = pdict['raw_position_object_id']
            self.insert1(key)

            MergedPosition.insert(self.fetch('KEY'), skip_duplicates=True)

    def fetch_nwb(self, *attrs, **kwargs):
        return fetch_nwb(self, (Nwbfile, 'nwb_file_abs_path'), *attrs, **kwargs)

    def fetch1_dataframe(self):
        raw_position_nwb = self.fetch_nwb()[0]['raw_position']
        return pd.DataFrame(
            data=raw_position_nwb.data,
            index=pd.Index(raw_position_nwb.timestamps, name='time'),
            columns=raw_position_nwb.description.split(', '))


@schema
class MethodTwoPosition(dj.Manual):
    definition = """Position data that is entered manually
    -> Session
    ---
    import_file_name: varchar(2000)  # path to import file
    """


@schema
class MergedPosition(dj.Manual):
    # adapted from https://github.com/ttngu207/db-programming-with-datajoint/blob/master/notebooks/pipelines_merging_design_master_part.ipynb
    # this table should be populated by the upstream position tables so that every entity in those tables
    # is referenced from an entity in a part table here. the upsteam position tables should use in make():
    # MergedPosition.insert(self.fetch('KEY'), skip_duplicates=True)

    definition = """
    -> merged_position_id: uuid
    """

    class RawPosition(dj.Part):
        definition = """
        -> master
        ---
        -> RawPosition
        """

    class MethodTwoPosition(dj.Part):
        definition = """
        -> master
        ---
        -> MethodTwoPosition
        """

    @property
    def all_joined(self):
        parts = self.parts(as_objects=True)
        primary_attrs = list(dict.fromkeys(itertools.chain.from_iterable([p.heading.names for p in parts])))

        query = dj.U(*primary_attrs) * parts[0].proj(..., **{a: 'NULL' for a in primary_attrs if a not in parts[0].heading.names})
        for part in parts[1:]:
            query += dj.U(*primary_attrs) * part.proj(..., **{a: 'NULL' for a in primary_attrs if a not in part.heading.names})

        return query

    @classmethod
    def insert(cls, rows, **kwargs):
        """
        :param rows: An iterable where an element is a dictionary.
        """

        try:
            for r in iter(rows):
                assert isinstance(r, dict), 'Input "rows" must be a list of dictionaries'
        except TypeError:
            raise TypeError('Input "rows" must be a list of dictionaries')

        parts = cls.parts(as_objects=True)
        master_entries = []
        parts_entries = {p: [] for p in parts}
        for row in rows:
            key = {}
            for part in parts:
                parent = part.parents(as_objects=True)[-1]
                if parent & row:
                    if not key:
                        key = (parent & row).fetch1('KEY')
                        master_key = {cls.primary_key[0]: dj.hash.key_hash(key)}
                        parts_entries[part].append({**master_key, **key})
                        master_entries.append(master_key)
                    else:
                        raise ValueError(f'Mutual Exclusivity Error! Entry exists in more than one parent table - Entry: {row}')

            if not key:
                raise ValueError(f'Non-existing entry in any of the parent tables - Entry: {row}')

        with cls.connection.transaction:
            super().insert(cls(), master_entries, **kwargs)
            for part, part_entries in parts_entries.items():
                part.insert(part_entries, **kwargs)


@schema
class StateScriptFile(dj.Imported):
    definition = """
    -> TaskEpoch
    ---
    file_object_id: varchar(40)  # the object id of the file object
    """

    def make(self, key):
        """Add a new row to the StateScriptFile table. Requires keys "nwb_file_name", "file_object_id"."""
        nwb_file_name = key['nwb_file_name']
        nwb_file_abspath = Nwbfile.get_abs_path(nwb_file_name)
        nwbf = get_nwb_file(nwb_file_abspath)

        associated_files = nwbf.processing.get('associated_files') or nwbf.processing.get('associated files')
        if associated_files is None:
            print(f'Unable to import StateScriptFile: no processing module named "associated_files" '
                  f'found in {nwb_file_name}.')
            return

        for associated_file_obj in associated_files.data_interfaces.values():
            if not isinstance(associated_file_obj, ndx_franklab_novela.AssociatedFiles):
                print(f'Data interface {associated_file_obj.name} within "associated_files" processing module is not '
                      f'of expected type ndx_franklab_novela.AssociatedFiles\n')
                return
            # parse the task_epochs string
            # TODO update associated_file_obj.task_epochs to be an array of 1-based ints,
            # not a comma-separated string of ints
            epoch_list = associated_file_obj.task_epochs.split(',')
            # only insert if this is the statescript file
            print(associated_file_obj.description)
            if ('statescript'.upper() in associated_file_obj.description.upper() or
            'state_script'.upper() in associated_file_obj.description.upper() or
            'state script'.upper() in associated_file_obj.description.upper() ):
                # find the file associated with this epoch
                if str(key['epoch']) in epoch_list:
                    key['file_object_id'] = associated_file_obj.object_id
                    self.insert1(key)
            else:
                print('not a statescript file')

    def fetch_nwb(self, *attrs, **kwargs):
        return fetch_nwb(self, (Nwbfile, 'nwb_file_abs_path'), *attrs, **kwargs)


@schema
class VideoFile(dj.Imported):
    definition = """
    -> TaskEpoch
    video_file_num = 0: int
    ---
    video_file_object_id: varchar(40)  # the object id of the file object
    """

    def make(self, key):
        nwb_file_name = key['nwb_file_name']
        nwb_file_abspath = Nwbfile.get_abs_path(nwb_file_name)
        nwbf = get_nwb_file(nwb_file_abspath)
        video = get_data_interface(nwbf, 'video', pynwb.behavior.BehavioralEvents)

        if video is None:
            print(f'No video data interface found in {nwb_file_name}\n')
            return

        # get the interval for the current TaskEpoch
        interval_list_name = (TaskEpoch() & key).fetch1('interval_list_name')
        valid_times = (IntervalList & {'nwb_file_name': key['nwb_file_name'],
                                       'interval_list_name': interval_list_name}).fetch1('valid_times')

        for video_obj in video.time_series.values():
            # check to see if the times for this video_object are largely overlapping with the task epoch times
            if len(interval_list_contains(valid_times, video_obj.timestamps) > .9 * len(video_obj.timestamps)):
                key['video_file_object_id'] = video_obj.object_id
                self.insert1(key)

    def fetch_nwb(self, *attrs, **kwargs):
        return fetch_nwb(self, (Nwbfile, 'nwb_file_abs_path'), *attrs, **kwargs)


@schema
class HeadDir(dj.Imported):
    definition = """
    -> Session
    ---
    nwb_object_id: int    # the object id of the data in the NWB file
    -> IntervalList       # the list of intervals for this object
    """

    def make(self, key):
        nwb_file_name = key['nwb_file_name']
        nwb_file_abspath = Nwbfile.get_abs_path(nwb_file_name)
        nwbf = get_nwb_file(nwb_file_abspath)

        # position data is stored in the Behavior processing module
        behav_mod = nwbf.processing.get('behavior')
        if behav_mod is None:
            print(f'Unable to import HeadDir: no processing module named "behavior" in {nwb_file_name}.')
            return

        headdir_obj = behav_mod.get('Head Direction')
        if headdir_obj is None:
            print('No conforming head direction data found.')
            return

        # TODO do something with headdir_obj
        key['nwb_object_id'] = -1
        key['interval_list_name'] = 'task epochs'  # this is created when we populate the Task schema
        self.insert1(key)


@schema
class Speed(dj.Imported):
    definition = """
    -> Session
    ---
    nwb_object_id: int    # the object id of the data in the NWB file
    -> IntervalList       # the list of intervals for this object
    """

    def make(self, key):
        nwb_file_name = key['nwb_file_name']
        nwb_file_abspath = Nwbfile.get_abs_path(nwb_file_name)
        nwbf = get_nwb_file(nwb_file_abspath)

        # position data is stored in the Behavior processing module
        behav_mod = nwbf.processing.get('behavior')
        if behav_mod is None:
            print(f'Unable to import Speed: no processing module named "behavior" in {nwb_file_name}.')
            return

        speed_obj = behav_mod.get('Speed')
        if speed_obj is None:
            print('No conforming speed data found.')
            return

        # TODO do something with speed_obj
        key['nwb_object_id'] = -1
        key['interval_list_name'] = 'task epochs'  # this is created when we populate the Task schema
        self.insert1(key)


@schema
class LinPos(dj.Imported):
    definition = """
    -> Session
    ---
    nwb_object_id: int    # the object id of the data in the NWB file
    -> IntervalList       # the list of intervals for this object
    """

    def make(self, key):
        nwb_file_name = key['nwb_file_name']
        nwb_file_abspath = Nwbfile.get_abs_path(nwb_file_name)
        nwbf = get_nwb_file(nwb_file_abspath)

        # position data is stored in the Behavior processing module
        behav_mod = nwbf.processing.get('behavior')
        if behav_mod is None:
            print(f'Unable to import LinPos: no processing module named "behavior" in {nwb_file_name}.')
            return

        linpos_obj = behav_mod.get('Linearized Position')
        if linpos_obj is None:
            print('No conforming linearized position data found.')
            return

        # TODO do something with linpos_obj
        key['nwb_object_id'] = -1
        key['interval_list_name'] = 'task epochs'  # this is created when we populate the Task schema
        self.insert1(key)
