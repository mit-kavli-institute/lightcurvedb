class IngestReport(object):
    def __init__(self, start, end, file_proc_elapsed, mapping_elapsed, ingest_elapsed, merged_lcs, new_lcs):
        self.start_time = start
        self.end_time = end
        self.file_time = file_proc_elapsed
        self.mapping_time = mapping_elapsed
        self.ingest_elapsed = ingest_elapsed
        self.merged = merged_lcs
        self.inserted = new_lcs

    def __repr__(self):
        print('INGEST REPORT'.center(25, '-'))
        print(f'File Processing time: {self.file_time}')
        print(f'Mapping time:         {self.mapping_time}')
        print(f'Ingestion time:       {self.ingestion_elapsed}')
        print(f'Total time:           {self.end_time - self.start_time}')
        print('-'*25)
        print(f'# of NEW lightcurves:     {len(self.inserted)}')
        print(f'# of MERGED lightcurves:  {len(self.merged)}')
        print(f'Avg time per lc:          {self.avg_time_per_lc}/s')

    @property
    def avg_time_per_lc(self):
        total = len(self.inserted) + len(self.merged)
        return total / (self.end_time - self.start_time).total_seconds()
