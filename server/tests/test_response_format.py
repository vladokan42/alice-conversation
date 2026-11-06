"""Real upstream compatibility: an outer code fence is not speech markup."""
import unittest
import server
class DecisionFormatTests(unittest.TestCase):
 def test_single_json_code_fence_is_decoded_not_spoken(self):
  raw='```json\n{"speak": true, "text": "Давно пора, блять, а то развели тут демагогию на пустом месте."}\n```'
  self.assertEqual(server.parse_decision(raw),'Давно пора, блять, а то развели тут демагогию на пустом месте.')
 def test_code_fence_does_not_relax_schema_or_spoken_text(self):
  for raw in ('prefix\n```json\n{"speak":true,"text":"Да."}\n```','```json\n{"speak":true,"text":"Да."}\n```\nsuffix','```json\n{"speak":true,"text":"`код`"}\n```','```json\n{"speak":true,"text":"Думаю, да."}\n```','```json\n{"speak":true,"text":"Да.","action":"run"}\n```','```json\n{"speak":true,"text":"Да."}\n','```json\n{"speak":true,"text":"Да."}{"speak":true,"text":"Нет."}\n```'):
   with self.subTest(raw=raw):self.assertIsNone(server.parse_decision(raw))
