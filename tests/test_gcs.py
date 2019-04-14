import unittest
import mock


class listGCS_TestCase(unittest.TestCase):
    output = [
        {
            'file_name': 'test.csv',
            'content_type': 'application/octet-stream',
            'size': 13618
        }
    ]

    @mock.patch('c3po.gcp.storage.listGCS', return_value=output)
    def test_rm(self, mockList):
        self.data = mockList(
            credentials="~/.gcloud/keyfile.json",
            project="project",
            bucket="bucket"
        )
        assert len(self.data[0].keys()) == 3
