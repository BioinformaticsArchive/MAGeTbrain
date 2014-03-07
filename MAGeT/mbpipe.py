#!/usr/bin/env python2.7
# vim: set ts=2 sw=2:
import logging
from itertools import chain
import datetime
import random
import subprocess
import os, os.path
import glob
import string
import sys
import stat
import errno
from collections import defaultdict
from os.path import join, exists, basename, dirname

STAGE_NONE      = 'NONE'         # not a stage

## Logging
class SpecialFormatter(logging.Formatter):
  FORMATS = {logging.DEBUG :"DBG: MOD %(module)s: LINE %(lineno)d: %(message)s",
             logging.ERROR : "ERROR: %(message)s",
             logging.INFO : "%(message)s",
             'DEFAULT' : "%(levelname)s: %(message)s"}

  def format(self, record):
    self._fmt = self.FORMATS.get(record.levelno, self.FORMATS['DEFAULT'])
    return logging.Formatter.format(self, record)

hdlr = logging.StreamHandler(sys.stderr)
hdlr.setFormatter(SpecialFormatter())
logging.root.addHandler(hdlr)
logging.root.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

def random_string(self, length = 4): 
  return str(uuid4()).replace('-', '')[:length]

### Pipeline construction
class CommandQueue(object):
  def __init__(self):
    self.dry_run = False
    self.name_length = 16

  def set_dry_run(self, state):
    self.dry_run = state

  def _runnable_command(self, command, name = None): 
    """Convert a tuple to string, and taskset to script"""
    if isinstance(command, tuple): 
      return " ".join(map(str, command))
    if isinstance(command, taskset): 
      script = name or self._get_script_name()
      #script = '{}-{}.sh'.format(stage,index)
      command.write_to_script(script)
      os.chmod(script, stat.S_IRWXU)
      return script
    return command

  def make_dirs(self, outputfiles): 
    dirs = set(map(lambda x: x.dirname, outputfiles))
    logger.debug("Making directories: \n\t" + "\n\t".join(dirs))
    if not self.dry_run:
      map(mkdirp, dirs)
    
  def run(self, tasklist):
    """Runs the tasklist"""
    flat = tasklist.flatten()

    # make output directories
    self.make_dirs(flat.get_all_outputfiles())

    # execute the incomplete stages
    for stage in flat.stage_order:
      for index, command in enumerate(flat.get_commands(stage, False)):
        self._execute(self._runnable_command(command))

  def _execute(self, command, input = ""):
    """Spins off a subprocess to run the cgiven command"""
    if input:
      logger.debug("exec: {0}\n\t{1}".format(command, input.replace('\n','\n\t')))
    else:
      logger.debug("exec: " + command)

    if self.dry_run:
      return
    proc = subprocess.Popen(command.split(),
             stdin = subprocess.PIPE, stdout = 2, stderr = 2)
    proc.communicate(input)
    if proc.returncode != 0:
      raise Exception("Returns %i :: %s" %( proc.returncode, command ))

class ParallelCommandQueue(CommandQueue):
  def __init__(self, processors = 8):
    CommandQueue.__init__(self)
    self.processors = processors

  def run(self, tasklist):
    flat = tasklist.flatten()

    # make output directories
    self.make_dirs(flat.get_all_outputfiles())

    for stage in flat.stage_order:
      commands = flat.get_commands(stage, complete=False)
      commands = map(lambda c: self._runnable_command(c), commands)
      if commands:
        self.parallel(commands)

  def parallel(self, commands):
    "Runs the list of commands through parallel"
    command = 'parallel -j%i' % self.processors
    self._execute(command, input='\n'.join(commands))

class QBatchCommandQueue(CommandQueue):
  def __init__(self, processors = 8, batch='pbs',hints=None,walltime='1:00:00'):
    assert batch in ['pbs','sge']
    CommandQueue.__init__(self)
    self.processors = processors
    self.batch = batch
    self.hints = hints or dict()
    self.walltime = walltime

  def run(self, tasklist):
    previous_stage = ""

    run_id = random_string(length=5)

    # make output directories
    self.make_dirs(tasklist.get_all_outputfiles())

    for stage in tasklist.stage_order:
      commands = tasklist.get_commands(stage, complete=False)
      if not commands:
        continue
      
      # unique name to avoid clashes
      stage_name= "{0}_{1}".format(stage, run_id)

      # get job meta data
      walltime = self.walltime
      if stage in self.hints and 'walltime' in self.hints[stage]:
        walltime   = self.hints[stage]['walltime']

      processors = self.processors
      if stage in self.hints and 'procs' in self.hints[stage]:
        processors = stage_queue_hints[stage]['procs']

      # convert any sub pipelines into scripts
      runnable_commands = []
      for index, command in enumerate(commands):
        runnable_commands.append(self._runnable_command(command,
            name="{}-{}.sh".format(stage_name,index)))

      # queue up the commands
      self.qbatch(runnable_commands,
          batch_name = stage_name, 
          afterok    = previous_stage+"*",
          walltime   = walltime, 
          processors = processors)

      previous_stage = stage_name 

  def qbatch(self, commands, batch_name = None, afterok=None, walltime="10:00:00", processors = None):
    logger.info('running {0} commands after stage {1}'.format(len(commands), afterok))

    opt_name    = batch_name and '-N {0}'.format(batch_name) or ''
    opt_afterok = afterok and '--afterok_pattern {0}'.format(afterok) or ''
    batchsize   = min(self.processors, processors)
    self._execute('qbatch --batch_system {0} {1} {2} - {3} {4}'.format(
        self.batch, opt_name, opt_afterok, batchsize, walltime),
        input='\n'.join(commands))

#### Guts
class Template:
  """Represents an MR image with labels, optionally"""
  def __init__(self, image, labels = None):
    image_path      = os.path.realpath(image)
    self.stem       = os.path.basename(os.path.splitext(image_path)[0])
    self.image      = image
    self.labels     = labels

    expected_labels = os.path.join(dirname(dirname(image_path)), 'labels', self.stem + "_labels.mnc")
    if not labels and os.path.exists(expected_labels):
      self.labels = expected_labels

  @classmethod
  def get_templates(cls, path):
    """return a list of MR image Templates from the given path.  Expect to find
    a child folder named brains/ containing MR images, and labels/ containing
    corresponding labels."""
    return [Template(i) for i in glob.glob(join(path, 'brains', "*.mnc"))]


# new style?
class datafile:
  def __init__(self, path):
    self.path = path
    self.abspath = os.path.abspath(path)
    self.basename = os.path.basename(path)
    self.dirname = os.path.dirname(path)
    self.realpath = os.path.realpath(path)
    self.stem = os.path.splitext(self.basename)[0]
  def exists(self):
    return os.path.isfile(self.realpath)
  def __str__(self):
    return self.abspath
  def __repr__(self):
    return self.realpath
  def __eq__(self, other):
    if isinstance(other,self.__class__):
      return os.path.realpath(self.abspath) == os.path.realpath(other.abspath)
    else:
      return false
  def __ne__(self,other):
    return not self.__eq__(other)
  def __hash__(self):
    return hash(os.path.realpath(self.abspath))

class out(datafile):
  pass

class image(datafile):
  def objects(self):
    return map(datafile, glob.glob('{0.dirname}/../objects/*.obj'.format(self)))
  def labels(self):
    return map(datafile,
        glob.glob('{0.dirname}/../labels/{0.stem}_labels.mnc'.format(self)))

class command:
  def __init__(self, fmt_string, *args, **kwargs):
    cmdstr = fmt_string.format(*args,**kwargs)
    self.__init__(shlex.split(cmdstr))  # split it up into parts
    #
    #todo: we could augment the usual string formatting syntax to include
    #      the ability to indicate types (e.g. that an argument is an 'out'put
    #      file.
    #
    #      Ideas:
    #         # 1. use <Type>@<string> to mean
    #         #   call Type(string), as in:
    #         nuc_correct {0} out@{output_dir}/nuc/{0.stem}
    #
    #         # Q: what if there are @ characters in the string?
    #         # A: too bad. :-)  But honestly, we could just check for strings
    #         that match $\w+@ so that the only strings that may get confused
    #         are those that happen to start with out@ and are NOT mean to be
    #         interpreted as types.
    #
    #         # 2. use the conversion option for output files: o
    #         bestlinreg {from} {to} {xfm!o} {resampled!o}

  def __init__(self, *args):
    #todo: sanity checks
    self.cmd = args

class taskset:
  def __init__(self):
    self.stages = defaultdict(list)
    self.stage_order = []
  def stage(self,stage_name):
    """syntactic sugar"""
    t = self
    class _stage():
      def command(self, *args):
        t.command(stage_name,args)
    return _stage()

  def command(self,stage,cmd):
    self.stages[stage].append(cmd)
    if stage not in self.stage_order: self.stage_order.append(stage)

  def set_stage_order(self, order):
    assert set(order).issubset(set(self.stages.keys)), "some stages given are not part of this taskset"
    self.stage_order = order

  def __str__(self):
    __str=""
    for stage in self.stage_order:
      __str += '{0}:\n'.format(stage)
      for command in self.stages[stage]:
        __str += '\t{0}\n'.format(' '.join(map(str,command)))
    return __str

  def _is_outputfile_complete(self, outputfile):
    return os.path.isfile(str(outputfile))

  def _is_command_complete(self, command): 
    if isinstance(command,taskset) and not command.is_complete(): 
      return False

    outputfiles = filter(lambda x: isinstance(x,out),command)
    if not all(map(lambda x: self._is_outputfile_complete(x), outputfiles)):
      return False

    return True

  def is_complete(self): 
    for command in self.stages.itervalues():
      if not self._is_command_complete(command):
        return false
    return true 

  def _filter_unfinished(self,all_commands):
    """returns only those commands that have not yet completed"""
    return filter(lambda x: not self._is_command_complete(x), all_commands)

  def get_commands(self, stage, complete=True):
    all_commands = self.stages[stage]
    if complete: 
      return all_commands
    else: 
      return self._filter_unfinished(all_commands)

  def get_all_outputfiles(self): 
    o = map(lambda x: self.get_command_outputfiles(x),
        chain(*self.stages.values()))
    return list(chain(*o))

  def get_command_outputfiles(self, command): 
    if isinstance(command,taskset):
      return command.get_all_outputfiles() 

    return filter(lambda x: isinstance(x,out),command)

  def flatten(self,tasklist = None, prefix = ""):
    tasklist    = tasklist or taskset()

    for stage in self.stage_order:
      stage_name  = prefix and "{}:{}".format(prefix,stage) or stage
      commands    = filter(lambda x: isinstance(x,tuple),   self.stages[stage])
      subtasksets = filter(lambda x: isinstance(x,taskset), self.stages[stage])

      for i in commands:
        tasklist.command(stage_name, i)
      for i in subtasksets:
        flat = i.flatten(tasklist, stage_name)
    return tasklist

  def write_to_script(self,scriptname,processes=8):
    script = open(scriptname,'w')
    script.write('#!/bin/bash\n')
    script.write('#PBS -l nodes=1:ppn=8,walltime=4:00:00\n')
    script.write('#PBS -V\n')
    script.write('#PBS -j oe\n')
    script.write("cd $PBS_O_WORKDIR\n")
    script.write(
        'echo "This script was generated by MAGeT morph on {0!s}"\n'.format(
          datetime.datetime.now()))

    flat = self.flatten()

    for stage in flat.stage_order:
      commands = flat.stages[stage]
      self._script_commands(script,processes,stage,commands,True)

    script.write('\necho "DONE!!!!"\n')
    script.close()

  def _script_commands(self,buffer,processes,stage_name,commands,use_parallel=True):
    """Given a buffer, write out the shell commands to run the given stage's commands"""
    if not commands:
      return 

    details = [(c,filter(lambda x: isinstance(x,out),c)) for c in commands]
    runnable, outputs = zip(*details)

    buffer.write('\necho "STAGE {0} -- creating directories"\n'.format(stage_name))
    dirs_to_make = set(map(lambda x: x.dirname, chain(*outputs)))
    buffer.write(''.join(map(lambda x: 'mkdir -p "{0}"\n'.format(x),
      dirs_to_make))+'\n')

    buffer.write('\necho "STAGE {0} -- commands"\n'.format(stage_name))
    shell_commands = map(lambda command: " ".join(map(str,command)), runnable)

    #TODO: shell escape shell commands
    if processes == 0:
      buffer.write('\n'.join(shell_commands) + '\n')
    elif not use_parallel:
      for i in range(0,len(shell_commands),processes):
        batch = shell_commands[i:i+processes]
        buffer.write(' &\n'.join(batch) + ' &\n')
        buffer.write('wait;\n')
    else:  # use parallel
      buffer.write("parallel -j{0} <<EOF\n".format(processes))
      buffer.write('\n'.join(shell_commands) + '\n')
      buffer.write('EOF\n')

### utility functions
def mkdirp(*p):
  """Like mkdir -p"""
  path = join(*p)
  try:
    os.makedirs(path)
  except OSError as exc:
    if exc.errno == errno.EEXIST:
      pass
    else: raise
  return path

def execute(command, input = ""):
  """Spins off a subprocess to run the cgiven command"""
  proc = subprocess.Popen(command.split(),
           stdin = subprocess.PIPE, stdout = 2, stderr = 2)
  proc.communicate(input)
  if proc.returncode != 0:
    raise Exception("Returns %i :: %s" %( proc.returncode, command ))

def parallel(commands, processors=8):
  "Runs the list of commands through parallel"
  command = 'parallel -j%i' % processors
  execute(command, input='\n'.join(commands))
