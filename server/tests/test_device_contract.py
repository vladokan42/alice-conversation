"""Cross-component contract: generation cancellation is not a topic reset."""
import unittest
from test_server import Clock, ASR, VAD, state, speech, quiet, finish_decision
import server

class ContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_speech_epoch_keeps_recognized_turn_and_context(self):
        clock=Clock(); seen=[]
        class Parts(ASR):
            first=True
            def accept(self,pcm):
                if any(pcm) and self.first:
                    self.first=False
                    return 'Обсуждаем планы'
                return ''
            def finish(self): return 'на завтра'
        async def decide(ctx):
            seen.append(ctx)
            return '{"speak":false,"text":""}'
        async def emit(value): pass
        engine=server.ConversationEngine(Parts(),VAD(),decide,emit,clock=clock)
        await engine.start(background=False)
        try:
            engine.control(state())
            await speech(engine,clock)
            engine.control({'type':'cancel','epoch':1})
            engine.control(state(epoch=2))
            await speech(engine,clock)
            await quiet(engine,clock)
            await finish_decision(engine)
            self.assertTrue(seen)
            self.assertIn('Обсуждаем планы на завтра',seen[-1][-1])
        finally: await engine.close()

    async def test_delayed_reset_intent_clears_new_epoch_context(self):
        clock=Clock()
        async def emit(value): pass
        engine=server.ConversationEngine(ASR(),VAD(),lambda c:None,emit,clock=clock)
        await engine.start(background=False)
        try:
            engine.control(state(epoch=3))
            engine.context.append((clock(),'Текущая тема'))
            engine.control({'type':'reset','epoch':1})
            self.assertFalse(engine.context)
            self.assertEqual(engine.epoch,3)
        finally: await engine.close()
