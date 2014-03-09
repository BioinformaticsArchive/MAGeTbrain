#/usr/bin/env python2.7
# vim: set ts=2 sw=2:
import MAGeT
from MAGeT.morpho import *
from MAGeT.mbpipe import *
import unittest

class TestMorphobits(unittest.TestCase):
#  def setUp(self):
#    self.morpho = morpho = Morphobits()
#    morpho.config('output_dir','output')
#    morpho.config('reg_dir',   'registrations')
#
#    model     = image('input/atlas1.mnc')
#    atlases   = [model]
#    subjects  = [image('input/subject1.mnc')]
#    templates = subjects
#    self.model_only_1_subject = [atlases,subjects,templates,model]
#
#  def test_model_only_single_subject_sanity_checks(self):
#    tasklist  = self.morpho.build_pipeline(*self.model_only_1_subject)
#
#    for stage_name, count in {'image.prep'            : 2,  # autocrop and nuc
#                              'subject.to.modelspace' : 1,
#                              'subject.to.modelspace' : 1, 
#                              'pairwise.reg'          : 1, 
#                              'model.subject.xfm'     : 2,  # nl and nl-only xfms
#                              'model.subject.gridavg' : 1 , 
#                              'model.subject.displace': 1}.iteritems():
#
#      stage = tasklist.stages[stage_name]
#      self.assertTrue(stage, msg=stage_name + ' - ' + repr(stage))
#      self.assertEquals(len(stage), count, 
#          msg="Stage {} has {} commands, but only {} expected: {}".format(
#            stage_name, len(stage), count, "\n".join(map(str,stage))))

  def test_command_var(self): 
    c = command("Thanks for all the fish", **locals())
    self.assertEquals(str(c), "Thanks for all the fish")

    thing = 'cats'
    c = command("Thanks for all the {thing}", **locals())
    self.assertEquals(str(c), "Thanks for all the cats")

  def test_pipeline_command_expansion(self): 
    p  = pipeline()
    p.command('stage1','Thanks for all the fish', **locals())
    c = p.stages['stage1'][0]
    self.assertEquals(str(c), "Thanks for all the fish")

    thing = 'cats'
    p.command('stage2', 'Thanks for all the {thing}', **locals())
    c = p.stages['stage2'][0]
    self.assertEquals(str(c), "Thanks for all the cats")

  def test_datafile_format(self):
    c = datafile('/path/to/{thing}')
    d = c.format(thing='somewhere')
    self.assertEquals(c.path, '/path/to/somewhere')
    self.assertEquals(d.path, '/path/to/somewhere')

  def test_command_with_objects(self):
    c = command('cmd one ', datafile('foo'))
    self.assertEquals('cmd one foo',str(c))

if __name__ == '__main__':
  unittest.main()

