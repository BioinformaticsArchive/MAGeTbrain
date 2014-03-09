#!/usr/bin/env python2.7
# vim: set ts=2 sw=2:
from itertools import product, chain
from collections import defaultdict
from multiprocessing.dummy import Pool
from subprocess import call
import os, errno, random, glob, sys
import os.path
import argparse
import string
from mbpipe import *

class Morphobits:
  """The vanilla pipeline for surface morphology analysis"""

  def __init__(self):
    #todo: fugly, replace with members
    self._config = dict() # runtime configuration

  def xfmpath(self,images,xfmname='nl.xfm'):
    """Returns path to an XFM file that warps through a series of image spaces

       If image A has been registered to image B, then we store the XFM at:
          <regdir>/A/B/<xfmname>
       That is, this is a path to an XFM that warps from A->B space.

       In general, an XFM may represent a series of warps through image spaces
       (e.g. from A->B->C->...->X space), and the path returned is:
          <regdir>/A/B/C/.../X/<xfmname>
    """
    args=[self._config['reg_dir']] + [i.stem for i in images] + [xfmname]
    return os.path.join(*args)

  def config(self, key, value=None):
    if value:
      self._config[key] = value
    else:
      value = self._config.get(key,None)
    return value

  def build_pipeline(self,atlases,native_subjects,native_templates,model,p=None):
    p = p or pipeline()
    subjects = []
    templates = []
    output_dir=self.config('output_dir')

    ## BEGIN
    res_model='output/modelspace/'+model.basename
    p.command('prep',
      'autocrop -isostep 1 {model}',out(res_model), **vars())

    # move to subjects and templates to model space
    for s in set(native_subjects).union(set(native_templates)):
      nuc = 'output/nuc/{s.basename}'
      xfm = self.xfmpath([model,s],xfmname='lsq12.xfm')
      stx = 'output/modelspace/{s.basename}'.format(s=s)

      p.command('image.prep', \
        'nu_correct -quiet -iter 100 -stop 0.0001 -fwhm 0.1 {s}',
        out(nuc), **vars())

      p.command('reg.subject.to.modelspace', \
        'bestlinreg -noverbose -lsq12 {nuc}',out(res_model),out(xfm), 
        **vars())

      p.command('subject.to.modelspace', \
        'mincresample -sinc -like {res_model} -2 -transform {xfm} {nuc}',
        out(stx), **vars())


      if s in native_subjects:  subjects.append(image(stx))
      if s in native_templates: templates.append(image(stx))

    # do pairwise registrations between atlases and templates and subjects
    for x,y in chain(product(atlases,templates), product(templates,subjects)):
      if x != y:
        p.command('pairwise.reg', 'mb_register',x,y,out(self.xfmpath([x,y])),
            **vars())

    for s in subjects:
      subject_tasklist = self.process_subject(model,atlases,templates,s)
      p.command('subject', subject_tasklist, **vars())
    return p

  def process_subject(self,model,atlases,templates,subject):
    p = pipeline()

    output_dir=self.config('output_dir')

    # do stuff with XFMs on all possible paths to subject
    grids=list()
    objects=defaultdict(list)
    gridavg='{output_dir}/gridavg/{subject.stem}_grid.mnc'.format(**vars())

    # compute all possible XFM paths to subject
    for a,t in product(atlases,templates):
      xfm         = self.xfmpath([model,a,t,subject])
      nl_only_xfm = self.xfmpath([model,a,t,subject],xfmname="nl_only.xfm")
      xfmdir      = os.path.dirname(xfm)
      grid        = '{xfmdir}/grid.mnc'.format(**vars())

      # concat a single XFM for pathway (skip moves from identical images)
      xfms=[]

      # handle the model->atlas case where XFMs are supplied
      if model != a:
        xfms.append(model.atlas_xfms[a]) # we guarantee existence earlier on

      for source,target in [(a,t),(t,subject)]:
        if source == target:
          continue
        xfms.append(self.xfmpath([source,target]))

      assert xfms is not [], \
        "{model}, {a}, {t}, {subjec} are all the same image?".format(**vars())

      # create two concatenated xfms: one with linear parts (to use with
      # segmentation), one without
      p.command('model.subject.xfm', \
        'xfmjoin',*(xfms + [out(xfm)]), **vars())
      p.command('model.subject.xfm', \
        'xfmjoin --nl-only',*(xfms + [out(nl_only_xfm)]), **vars())

      # compute non-lin grid for the entire path
      p.command('model.subject.displace', \
          'minc_displacement {model} {nl_only_xfm}',out(grid), **vars())

      # transform objects from atlases
      if self.config('surfaces'):
        #TODO: should transform to native space?
        for o in a.objects():
          kind=o.stem
          object='${xfmdir}/${kind}'.format(**vars())
          p.command('model.subject.objects.transform',
            'transform_objects {o} {xfm}',out(object), **vars())
          objects[kind].append(object)
      grids.append(grid)

    # compute average grid over all pathways to the subject
    p.command('model.subject.gridavg', \
      'mincaverage',*(grids + [out(gridavg)]), **vars())

    # calculate displacement of model objects
    displacement_dir='{output_dir}/displacement'.format(**vars())
    for o in model.objects():
      obj_displace='{displacement_dir}/{subject.stem}_{o.stem}.txt'.format(
          **vars())
      p.command('model.subject.displacement', 
        'object_volume_dot_product {o} {gridavg}',out(obj_displace), **vars())

    # operate on model..subject surfaces
    if self.config('surfaces'):
      for kind in objects.keys():
        # median surface per subject
        kind_dir  = '{output_dir}/{kind}'.format(**vars())
        medianobj = '{kind_dir}/{subject.stem}_median.obj'.format(**vars())
        voronoi   = '{kind_dir}/{subject.stem}_vorn.txt'.format(**vars())
        vorn_sa   = '{kind_dir}/{subject.stem}_vorn_SA.txt'.format(**vars())

        p.command('objects.median', \
          "make_median_surfaces.pl",out(medianobj),*objects[kind], **vars())
        p.command('objects.normals', \
          "recompute_normals",medianobj,medianobj) #fix: not out()

        if self.config('voronoi'):
          # voronoi of median surface
          p.command('objects.voronoi', \
            'depth_potential -area_voronoi {medianobj}',out(voronoi), **vars())

          #hack: determine the blurring kernal to use:
          kernel = kind.startswith('gp_') and 3 or 5
          vorn_blur = '{kind_dir}/{subject.stem}_vorn_SA_{kernel}mm_blur.txt'.format(
              **vars())
          #todo: blur surface

      #TODO: for segmentation, remember to transform voted volumes back into native space
    return p

def parse_args():
  parser = argparse.ArgumentParser()
  parser.add_argument("-n", dest="dry_run", default=False,
    action="store_true", help="Dry run. Show what would happen.")
  parser.add_argument("-j", "--processes", default=8,
    type=int, metavar='N', help="Number of processes to parallelize over.")
  parser.add_argument("-s", '--subjects', nargs='+', default=None,
    help="A list of subjects to use (name of the brain image without .mnc extension)")
  parser.add_argument("-t", '--templates', nargs='+', default=(),
    help="A list of templates to use (name of the subject without .mnc extension)")
  parser.add_argument('-q', '--queue', choices=['parallel', 'pbs', 'sge'],
    default = 'parallel', help="Queueing method to use")
  parser.add_argument('-w', '--walltime', default = '1:00:00',
    help="Default walltime for submitted jobs")
  parser.add_argument("--output_dir", default="output",
    type=os.path.abspath, metavar='',
    help="Top-level folder for all output")
  parser.add_argument("--input_dir",   default="input",
    type=os.path.abspath, metavar='',
    help="Directory containing atlas, template and subject libraries")
  parser.add_argument("--reg_dir",   default="output/registrations",
    type=os.path.abspath, metavar='',
    help="Directory containing registrations")
  parser.add_argument('--show-pipeline', default=False, action="store_true",
    help="Show pipeline.")
  parser.add_argument('--write-to-script', default=False, action="store_true",
    help="Write a single script which reproduces the pipeline.")

  # hidden options, for ninjas
  parser.add_argument('--surfaces', default=False, action="store_true",
    help=argparse.SUPPRESS) #help="Compute median surfaces.")
  parser.add_argument('--voronoi', default=False, action="store_true",
    help=argparse.SUPPRESS) #help="Compute voronoi partition of the median surface.")
  return parser.parse_args()

def user_error_if(condition, message):
  if condition:
    print >> sys.stderr, message
    sys.exit(1)

def main():
  options = parse_args()
  options.model_dir  = '{input}/model'.format(input=options.input_dir)

  morpho  = Morphobits()
  morpho.config('output_dir',options.output_dir)
  morpho.config('reg_dir',   options.reg_dir)
  morpho.config('voronoi',   options.voronoi)
  morpho.config('surfaces',  options.surfaces)

  # Get the model image
  inmodeldir = glob.glob('{model}/brains/*.mnc'.format(model=options.model_dir))
  user_error_if(
    len(inmodeldir) != 1, "Expected one model image in {dir}/brains, but found {count}".format(
      dir=options.model_dir, count=len(inmodeldir)))
  model = image(inmodeldir.pop())


  # check that the model has objects (if needed)
  user_error_if(not model.objects(),
      "Model objects expected in {0}, but found none.".format(options.model_dir))

  # get atlases
  atlases = map(image, glob.glob('input/atlases/brains/*.mnc'))
  if len(atlases) == 0:
    print >> sys.stderr, "WARNING: no atlases found, using model: {0}".format(model.abspath)
    atlases = [model]

  # todo: check that atlases have labels, objects, etc...

  # get model->atlas xfms
  model_atlas_xfms = dict()
  for atlas in atlases:
    if atlas != model:
      xfm='{model.dirname}/../xfms/to_{atlas.stem}.xfm'.format(model=model,atlas=atlas)
      xfm=os.path.abspath(xfm)
      user_error_if(not os.path.isfile(xfm),
        "Expected to find model->atlas xfm {0}".format(xfm))
      model_atlas_xfms[atlas] = xfm
  model.atlas_xfms = model_atlas_xfms  #HACK monkey patch my own code? yes.
  # todo: create a model class to handle much of the above

  # subjects
  subjects=map(image, glob.glob('{0}/subjects/brains/*.mnc'.format(options.input_dir)))
  user_error_if(not subjects, "No subjects found in {0}/subjects/brains/".format(
    options.input_dir))

  # templates
  if options.templates:
    templates = [s for s in subjects if s.stem in options.templates]
    user_error_if(not templates, "Templates specified not found in {dir}/subjects/brains/".format(
      dir=options.input_dir))
  else:
    templates=map(image, glob.glob('{0}/templates/brains/*.mnc'.format(options.input_dir)))
    user_error_if(not templates, "No templates found in {0}/templates/brains/".format(
      options.input_dir))

  # refine subject list
  if options.subjects:
    subjects = [s for s in subjects if s.stem in options.subjects]
  user_error_if(not subjects, "Subjects specified not found in {dir}/subjects/brains/".format(
    dir=options.input_dir))

  print 'model:\n\t{model.abspath}'.format(model=model)
  print 'atlases:\n\t', '\n\t'.join(map(str,atlases))
  print 'templates:\n\t', '\n\t'.join(map(str,templates))
  print 'subjects:\n\t', '\n\t'.join(map(str,subjects))
  p= morpho.build_pipeline(atlases,subjects,templates,model)

  if options.queue == "pbs":
    queue = QBatchCommandQueue(processors=options.processes, batch='pbs',
      walltime=options.walltime)
  elif options.queue == "sge":
    queue = QBatchCommandQueue(processors=options.processes, batch='sge',
      walltime=options.walltime)
  elif options.queue == "parallel":
    queue = ParallelCommandQueue(processors=options.processes)
  else:
    queue = CommandQueue()
  queue.set_dry_run(options.dry_run)

  if options.show_pipeline:
    print p

  if options.write_to_script:
    p.write_to_script('script.sh', options.processes)

  if not p.stages:
    print "Nothing to do."
  else:
    queue.run(p) 

if __name__ == '__main__':
  main()