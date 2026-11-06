"""Reason counters contain fixed labels only and never change reply gates."""
import asyncio
from collections import Counter
import json
import unittest

import server
from test_server import ASR, VAD, Clock, state, speech, quiet, finish_decision


class ParseDiagnostics(unittest.TestCase):
    def test_silent_invalid_and_spoken_results_are_distinguished_without_text(self):
        cases = [(json.dumps({'speak': False, 'text': ''}), None, 'decision_silent'),
                 ('not JSON', None, 'decision_rejected'),
                 (json.dumps({'speak': True, 'text': 'Проще можно.'}), 'Проще можно.', 'decision_speech')]
        for raw, expected, key in cases:
            counters = Counter()
            self.assertEqual(server.parse_decision(raw, counters=counters), expected)
            self.assertEqual(counters, {key: 1})
            self.assertNotIn('Проще можно', json.dumps(counters, ensure_ascii=False))


class EngineDiagnostics(unittest.IsolatedAsyncioTestCase):
    async def check_utterance(self, text, raw, action=None):
        clock, messages = Clock(), []
        async def emit(value):
            messages.append(value)
        async def decide(context):
            if action:
                action(engine, clock)
            return raw
        engine = server.ConversationEngine(ASR(text), VAD(), decide, emit, clock=clock)
        await engine.start(background=False)
        try:
            engine.control(state())
            await speech(engine, clock)
            await quiet(engine, clock)
            await finish_decision(engine)
            return dict(engine.counters), messages
        finally:
            await engine.close()

    async def test_successful_reply_has_all_stage_counts(self):
        counters, messages = await self.check_utterance('Можно сделать проще?', '{"speak":true,"text":"Убери лишнее."}')
        self.assertEqual(len(messages), 1)
        for key in ('utterances_finalized', 'llm_requests', 'decision_speech', 'replies'):
            self.assertEqual(counters.get(key), 1, key)
        self.assertGreater(counters.get('vad_speech_frames', 0), 0)
        self.assertNotIn('Можно', json.dumps(counters, ensure_ascii=False))

    async def test_valid_silence_is_not_an_error_or_rejected_json(self):
        counters, messages = await self.check_utterance('Понятная обычная фраза', '{"speak":false,"text":""}')
        self.assertEqual(messages, [])
        self.assertEqual(counters.get('decision_silent'), 1)
        self.assertNotIn('decision_rejected', counters)
        self.assertNotIn('llm_errors', counters)

    async def test_invalid_model_output_is_visible_without_body(self):
        counters, messages = await self.check_utterance('Понятная обычная фраза', 'INVALID PRIVATE RESPONSE')
        self.assertEqual(messages, [])
        self.assertEqual(counters.get('decision_rejected'), 1)
        self.assertNotIn('PRIVATE', json.dumps(counters))

    async def test_empty_and_filtered_asr_have_distinct_reasons(self):
        for text, key in [('', 'skip_empty_asr'), ('Алиса включи свет', 'skip_filtered_phrase')]:
            counters, messages = await self.check_utterance(text, '{"speak":true,"text":"Нельзя."}')
            self.assertEqual(messages, [])
            self.assertEqual(counters.get(key), 1)
            self.assertNotIn('llm_requests', counters)

    async def test_revision_change_drops_reply_without_changing_existing_rule(self):
        def cancel(engine, clock):
            engine.revision += 1
        counters, messages = await self.check_utterance('Понятная обычная фраза', '{"speak":true,"text":"Да."}', cancel)
        self.assertEqual(messages, [])
        self.assertEqual(counters.get('decision_speech'), 1)
        self.assertEqual(counters.get('drop_changed_after_llm'), 1)

    async def test_noise_after_llm_is_distinguishable_from_model_silence(self):
        def noisy(engine, clock):
            engine.silence = 0
        counters, messages = await self.check_utterance('Понятная обычная фраза', '{"speak":true,"text":"Да."}', noisy)
        self.assertEqual(messages, [])
        self.assertEqual(counters.get('decision_speech'), 1)
        self.assertEqual(counters.get('drop_not_quiet_after_llm'), 1)


if __name__ == '__main__':
    unittest.main()
