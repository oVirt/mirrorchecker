import unittest
import unittest.mock as mock

import mirror_checker


@mock.patch('mirror_checker.Mirror', autospec=True)
def mock_mirror_status(url, max_ts, whitelist, reachable, mock_mirror):
    mock_mirror.max_ts = max_ts
    mock_mirror.whitelist = whitelist
    mock_mirror.url = url
    mock_mirror.reachable = reachable
    return mock_mirror


class TestMirrorLoadFromFile(unittest.TestCase):
    def test_simple_load(self):
        config = "url='http://url1.com',interval=50,whitelist=True"
        with mock.patch('builtins.open', mock.mock_open(read_data=config)):
            res = mirror_checker._load_mirror_txt('file1')
            self.assertEqual(
                {
                    "url": "http://url1.com",
                    "interval": 50,
                    "whitelist": True,
                }, res[0]
            )

    def test_defaults_inserted(self):
        config = "url='http://url1.com'"
        with mock.patch('builtins.open', mock.mock_open(read_data=config)):
            res = mirror_checker._load_mirror_txt('file1')
            self.assertCountEqual(
                ['url', 'interval', 'whitelist'], list(res[0].keys())
            )

    def test_no_url_exception(self):

        config = ("url='http://url1.com'\n" "whitelist=True\n")

        with mock.patch('builtins.open', mock.mock_open(read_data=config)):
            self.assertRaises(
                ValueError, mirror_checker._load_mirror_txt, 'file1'
            )


class TestMirrorFiltering(unittest.TestCase):
    @mock.patch('time.time')
    def test_yum_threshold(self, mock_time):
        backend_no_init = object.__new__(mirror_checker.Backend)
        backend_no_init.configs = {'yum_threshold': 2}
        mock_time.return_value = 10
        mirrors = {
            'bad_unsynced':
            mock_mirror_status('bad_unsynced', 4, False, False),
            'good_synced_close':
            mock_mirror_status('good_synced_close', 8.1, False, True),
            'good_synced': mock_mirror_status('good_synced', 9, False, True)
        }
        expected = [key for key in mirrors.keys() if key.startswith('good')]
        self.assertCountEqual(
            [mirror.url for mirror in backend_no_init._filter(mirrors)],
            expected
        )

    @mock.patch('time.time')
    def test_unsynced_removed(self, mock_time):
        backend_no_init = object.__new__(mirror_checker.Backend)
        backend_no_init.configs = {'yum_threshold': 10}
        mock_time.return_value = 30
        mirrors = {
            'bad_unsynced':
            mock_mirror_status('bad_unsynced', 20, False, False),
            'good_synced': mock_mirror_status('good_synced', 21, False, True)
        }
        expected = [key for key in mirrors.keys() if key.startswith('good')]
        self.assertCountEqual(
            [mirror.url for mirror in backend_no_init._filter(mirrors)],
            expected
        )

    @mock.patch('time.time')
    def test_whitelist(self, mock_time):
        backend_no_init = object.__new__(mirror_checker.Backend)
        backend_no_init.configs = {'yum_threshold': 100}
        mock_time.return_value = 30
        mirrors = {
            'good_unsynced_whitelist':
            mock_mirror_status('good_unsynced_whitelist', 20, True, False),
            'good_synced': mock_mirror_status('good_synced', 11, False, True),
            'bad_unreachable':
            mock_mirror_status('bad_unreachable', 10, False, False)
        }
        expected = [key for key in mirrors.keys() if key.startswith('good')]
        self.assertCountEqual(
            [mirror.url for mirror in backend_no_init._filter(mirrors)],
            expected
        )

    @mock.patch('time.time')
    def test_custom_filter(self, mock_time):
        backend_no_init = object.__new__(mirror_checker.Backend)
        backend_no_init.configs = {'yum_threshold': 100}
        mock_time.return_value = 5
        mirrors = {
            'good_synced': mock_mirror_status('good_synced', 11, False, True),
            'bad_unreachable':
            mock_mirror_status('bad_unreachable', 10, False, False),
            'good_custom_filter':
            mock_mirror_status('good_custom_filter', 999, False, False)
        }
        custom_whitelist = [lambda mirror: mirror.max_ts == 999]
        expected = [key for key in mirrors.keys() if key.startswith('good')]
        self.assertCountEqual(
            [
                mirror.url
                for mirror in backend_no_init._filter(
                    mirrors=mirrors, custom_whitelist=custom_whitelist
                )
            ],
            expected
        )


if __name__ == '__main__':
    unittest.main()
