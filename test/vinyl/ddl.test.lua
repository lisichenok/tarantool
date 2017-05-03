fiber = require('fiber')

-- space secondary index create
space = box.schema.space.create('test', { engine = 'vinyl' })
index1 = space:create_index('primary')
index2 = space:create_index('secondary')
space:drop()

-- space index create hash
space = box.schema.space.create('test', { engine = 'vinyl' })
index = space:create_index('primary', {type = 'hash'})
space:drop()

-- new indexes on not empty space are unsupported
space = box.schema.space.create('test', { engine = 'vinyl' })
index = space:create_index('primary')
space:insert({1})
-- fail because of wrong tuple format {1}, but need {1, ...}
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })
#box.space._index:select({space.id})
space:drop()

space = box.schema.space.create('test', { engine = 'vinyl' })
index = space:create_index('primary')
space:insert({1, 2})
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })
#box.space._index:select({space.id})
space:drop()

space = box.schema.space.create('test', { engine = 'vinyl' })
index = space:create_index('primary')
space:insert({1, 2})
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })
#box.space._index:select({space.id})
space:delete({1})

-- must fail because vy_mems have data
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })
box.snapshot()
while space.index.primary:info().count ~= 0 do fiber.sleep(0.01) end

-- after a dump REPLACE + DELETE = nothing, so the space is empty now and
-- can be altered.
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })
#box.space._index:select({space.id})
space:insert({1, 2})
index:select{}
index2:select{}
space:drop()

space = box.schema.space.create('test', { engine = 'vinyl' })
index = space:create_index('primary', { run_count_per_level = 2 })
space:insert({1, 2})
box.snapshot()
space:delete({1})
box.snapshot()
while space.index.primary:info().run_count ~= 2 do fiber.sleep(0.01) end
-- must fail because vy_runs have data
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })

-- After compaction the REPLACE + DELETE + DELETE = nothing, so
-- the space is now empty and can be altered.
space:delete({1})
-- Make sure the run is big enough to trigger compaction.
space:insert({2, 3})
space:delete({2})
box.snapshot()
-- Wait until the dump is finished.
while space.index.primary:info().count ~= 0 do fiber.sleep(0.01) end
index2 = space:create_index('secondary', { parts = {2, 'unsigned'} })

space:drop()

--
-- gh-1632: index:bsize()
--
space = box.schema.space.create('test', { engine = 'vinyl' })
pk = space:create_index('primary', { type = 'tree', parts = {1, 'unsigned'}  })
for i=1,10 do box.space.test:replace({i}) end
box.space.test.index.primary:bsize() > 0

box.snapshot()
while space.index.primary:info().run_count ~= 1 do fiber.sleep(0.01) end

box.space.test.index.primary:bsize() == 0

space:drop()

--
-- gh-1709: need error on altering space
--
space = box.schema.space.create('test', {engine='vinyl'})
pk = space:create_index('pk', {parts = {1, 'unsigned'}})
space:auto_increment{1}
space:auto_increment{2}
space:auto_increment{3}
box.space._index:replace{space.id, 0, 'pk', 'tree', {unique=true}, {{0, 'unsigned'}, {1, 'unsigned'}}}
space:select{}
space:drop()

--
-- gh-2109: allow alter some opts of not empty indexes
--
space = box.schema.space.create('test', { engine = 'vinyl' })
pk = space:create_index('pk', {run_count_per_level = 3})
-- Create run and mem and then change page_size,
-- run_count_per_level and make dump.
space:auto_increment{1}
space:auto_increment{2}
space:auto_increment{3}
box.snapshot()
space:auto_increment{1}
space:auto_increment{2}
space:auto_increment{3}

pk:info().run_count
pk:info().page_size
pk:alter({page_size = pk:info().page_size * 2, run_count_per_level = 1})

space:auto_increment{1}
box.snapshot()
-- Wait for compaction. The compaction is starting because
-- the new run_count_per_level == 1 and there are two runs.
-- In old opts run_count_per_level was 3 and the compaction
-- would not stated.
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end
pk:info().run_count -- == 1, because new run_count_per_level == 1
pk:info().page_size
space:drop()

--
-- Ensure that runs with different page_sizes can be compacted.
--
space = box.schema.space.create('test', {engine='vinyl'})
page_size = 8192
range_size = 1024 * 1024 * 1024
pk = space:create_index('pk', {run_count_per_level = 10, page_size = page_size, range_size = range_size})
pad_size = page_size / 5
pad = string.rep('I', pad_size)
-- Create 4 pages with sizes 'page_size'
for i = 1, 20 do space:replace{i, pad} end
est_bsize = pad_size * 20
box.snapshot()
-- Wait end of the dump
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end
pk:info().page_count
pk:info().page_size
pk:info().run_count

-- Change page_size and trigger compaction
page_size = page_size * 2
pk:alter({page_size = page_size, run_count_per_level = 1})
pad_size = page_size / 5
pad = string.rep('I', pad_size)
-- Create 4 pages with new sizes in new run
for i = 1, 20 do space:replace{i + 20, pad} end
est_bsize = est_bsize + pad_size * 20
box.snapshot()
-- Wait for compaction
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end
pk:info().page_count
pk:info().page_size
pk:info().run_count
est_bsize / page_size == pk:info().page_count
space:drop()

--
-- Change range size to trigger split.
--
space = box.schema.space.create('test', {engine = 'vinyl'})
page_size = 64
range_size = page_size * 15
pk = space:create_index('pk', {page_size = page_size, range_size = range_size, run_count_per_level = 1})
pad = ''
for i = 1, 64 do pad = pad..(i % 10) end
for i = 1, 8 do space:replace{i, pad} end
box.snapshot()
while pk:info().run_count ~= 1 do fiber.sleep(0.01) end

-- Decrease the range_size and dump many runs to trigger split.
pk:alter({range_size = page_size * 2})
while pk:info().range_count < 2 do space:replace{1, pad} box.snapshot() fiber.sleep(0.01) end
space:drop()
