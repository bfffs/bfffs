#! /usr/bin/env ruby
# vim: ts=2:sw=2:expandtab

require 'optparse'
require 'ostruct'
require 'prime'
require 'set'

require 'bitset'
require 'narray'
require 'polynomial'

# A Ruby script that calculates performance figures for several declustered
# RAID layout algorithms.

LAYOUTS = %w(auto classic prime primes relpr ppl0 rpl)

# raised if the layout class can't be implemented with given parameters
class NotImplementableError < ArgumentError
end


# Polynomials in a Galois Field.  The coefficients are integers mod p
class GFPolynomial < Polynomial
  # Precomputed list of irreducible polynomials for all the most useful
  # configurations of PPL0.  Covers every PPL0 array where clustsize is <= 256
  # and n is > 1.  For n == 1, it would be better to use PRIME instead.
  # Precomputed polynomials thanks to Wolfram Alpha
  IRREDUCIBLES = {
    [2, 2] => [1,1,1],
    [2, 3] => [1,1,0,1],
    [2, 4] => [1,1,0,0,1],
    [2, 5] => [1,0,1,0,0,1],
    [2, 6] => [1,1,0,0,0,0,1],
    [2, 7] => [1,1,0,0,0,0,0,1],
    [2, 8] => [1,0,1,1,1,0,0,0,1],
    [3, 2] => [2,1,1],
    [3, 3] => [1,2,0,1],
    [3, 4] => [2,1,0,0,1],
    [3, 5] => [1,2,0,0,0,1],
    [5, 2] => [2,1,1],
    [5, 3] => [2,3,0,1],
    [7, 2] => [3,1,1],
    [11,2] => [7,1,1],
    [13,2] => [2,1,1]
  }

  # Create a new polynomial in GF(p^n), with coefs coefficients in increasing
  # order
  def initialize(p, n, coefs)
    @p = p
    @n = n
    super(coefs)
  end

  def substitute(x)
    @coefs = @coefs.map do |c|
      c %= @p
    end
    super(x)
  end

  def *(other)
    # XXX Remove hard-coded irreducible polynomial.
    irreducible = IRREDUCIBLES[[@p, @n]]
    remainder = super(other) % GFPolynomial.new(@p, @n, irreducible)
    GFPolynomial.new(@p, @n, remainder.coefs)
  end
end


# Return's Euler's Totient function evaluated at n
def totient(n)
  factors = Prime.prime_division(n)
  factors.inject(1) do |accum, factor|
    accum * (n - n / factor[0])
  end / n**(factors.size - 1)
end

# Inverse of a mod n
def invmod(a, n)
  t=0
  r=n
  newt=1
  newr=a
  while newr != 0
    quotient = r / newr
    (t, newt) = [newt, t - quotient * newt]
    (r, newr) = [newr, r - quotient * newr]
  end
  if r > 1
    raise ArgumentError.new("#{a} is not invertible mod #{n}")
  end
  if t < 0
    t = t + n
  end
  t
end

class Layout
  attr_reader :clustsize
  attr_reader :stripesize
  attr_reader :protection

  def initialize(clustsize, stripesize, protection)
    @clustsize = clustsize
    @stripesize = stripesize
    @protection = protection
  end

  # The number of rows in the layout.
  def depth
    raise NotImplementedError
  end

  # Name of the layout as used in the literature
  def name
    raise NotImplementedError
  end

  # Maximum percentage of any one disk that must be read to reconstruct the
  # contents of any other disk, for p failures.  Defined by Schwabe &
  # Sutherland's 2014 paper for the case of protection == 1
  def reconstruction_workload(p)
    raise NotImplementedError
  end

  # Maximum number of chunks that must be read from any one disk in order to
  # service a user read of the given number of chunks
  def parallel_read_count(chunks)
    raise NotImplementedError
  end

  # Return the physical location of a logical address.  Returns [disk index,
  # offset] for the given logical address.  The chunkaddr the 0-indexed number
  # of the chunk to locate: it is not an LBA or a byte offset.
  def userloc(chunkaddr)
    raise NotImplementedError
  end

  # Return a triple (chunk address, ischeck, checkidx).  chunk_address is the
  # user chunk address that resides at the given disk and offset, or the first
  # address of the stripe for parity chunks.  ischeck is true if the chunk is a
  # parity chunk.  checkidx is the check block index for parity chunks, or
  # dontcare for data chunks.
  def invloc(disk, offset)
    raise NotImplementedError
  end

  # Return the physical location of a parity block.  The userchunk addr is any
  # chunk address for a user chunk that is protected by this check chunk.
  # checkidx is the index of the check chunk for this stripe, and must be in
  # the range [0, @protection)
  def checkloc(userchunkaddr, checkidx)
  end

  # Size of the layout in stripe units
  def size
    depth * @clustsize
  end

  def stripes
    size / @stripesize
  end

  # Size of the layout in user chunks (not parity chunks)
  def userchunks
    stripes * (@stripesize - @protection)
  end
end

# Traditional RAID50, RAID60, or RAID60/3 with rotated data and parity.  Some
# systems don't rotate the data, and some do.  I don't think it matters.
class Classic < Layout
  def initialize(clustsize, stripesize, protection)
    unless clustsize % stripesize == 0
      raise NotImplementableError.new
    end
    super(clustsize, stripesize, protection)
    @m = stripesize - protection
    # In ZFS, each group would be a top-level vdev
    @groups = clustsize / stripesize
  end

  def depth
    @stripesize
  end

  def name
    "Classic"
  end

  def reconstruction_workload(failures)
    # Each disk must be read stripesize times if it's in the affected group, or
    # 0 times otherwise.
    (@stripesize - @protection + failures).to_f / depth
  end

  def parallel_read_count(chunks)
    @groups == 1 ? 1 : 2
  end

  def userloc(a)
    # Which stripe is it in?
    stripe = a / @m
    # Offset within it's stripe
    stripe_ofs = a % @m
    # Which group is it in?
    group = stripe % @groups
    groupstripe = stripe / @groups
    [group * @stripesize + (stripe_ofs - groupstripe) % @stripesize, groupstripe]
  end

  def checkloc(a, i)
    # Which stripe is it in?
    stripe = a / @m
    # Offset within it's stripe
    stripe_ofs = @m + i
    # Which group is it in?
    group = stripe % @groups
    groupstripe = stripe / @groups
    [group * @stripesize + (stripe_ofs - groupstripe) % @stripesize, groupstripe]
  end

  def invloc(disk, offset)
    group = disk / stripesize
    groupdisk = disk % stripesize
    groupstripe = offset
    stripe = group + @groups * offset
    stripestart = ((-groupstripe) % @clustsize) + group * stripesize
    stripeoffset = (groupdisk - stripestart) % @stripesize
    if stripeoffset >= @m
      [stripe * @m, true, stripeoffset - @m]
    else
      [stripe * @m + stripeoffset, false, 0]
    end
  end
end

class Ideal < Layout
  # The maximum percentage of chunks for any one disk that are parity chunks
  def max_parity
    @protection.to_f / @stripesize.to_f
  end

  # XXX I'm not postive this is correct when failures > 1
  def reconstruction_workload(failures)
    (@stripesize - @protection).to_f / (@clustsize - failures).to_f
  end

  def parallel_read_count(chunks)
    (chunks.to_f / @clustsize).ceil
  end
end

class PrimeLayout < Layout
  def initialize(clustsize, stripesize, protection)
    unless clustsize.prime?
      raise NotImplementableError.new
    end
    super(clustsize, stripesize, protection)
    @m = stripesize - protection
  end

  def depth
    @stripesize * (@clustsize - 1)
  end

  def name
    "PRIME"
  end

  # PRIME achieves the ideal reconstruction workload when failures ==
  # protection.  The paper did not consider other cases
  def reconstruction_workload(failures)
    (@stripesize - 1.0) / (@clustsize - 1.0)
  end

  def parallel_read_count(chunks)
    (chunks.to_f / @clustsize).ceil + 1
  end

  def userloc(chunkaddr)
    [disk(chunkaddr), offset(chunkaddr)]
  end

  def checkloc(chunkaddr, checkidx)
    [checkdisk(chunkaddr, checkidx), checkoffset(chunkaddr, checkidx)]
  end

  def invloc(disk, offset)
    k = @stripesize
    n = @clustsize
    f = @protection
    z = offset / k
    y = z % (n - 1) + 1
    if offset % k >= @m
      ischeck = true
      i = offset - (z + 1) * k + f
      a = @m * (n * z + (((disk * invmod(y, n) - i) * invmod(@m, n) - 1) % n))
    else
      ischeck = false
      a = (disk * invmod(y, n)) % n + n * (offset - f * z)
      i = 0
    end
    return [a, ischeck, i]
  end

  private
  def disk(a)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    y = (z % (@clustsize - 1)) + 1  # the stride
    (a * y) % @clustsize
  end

  def offset(a)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    (a / @clustsize).floor + protection * z
  end

  def checkdisk(a, i)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    y = (z % (@clustsize - 1)) + 1  # the stride
    ((((a / @m).floor + 1)*@m + i) * y) % @clustsize
  end

  def checkoffset(a, i)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    (z + 1) * @stripesize - @protection + i
  end
end

# A variant of prime optimized for SMR drives.  The layout is monotonic
class PrimeS < Layout
  def initialize(clustsize, stripesize, protection)
    unless clustsize.prime?
      raise NotImplementableError.new
    end
    super(clustsize, stripesize, protection)
    @m = stripesize - protection
  end

  def depth
    @stripesize * (@clustsize - 1)
  end

  def name
    "PRIME-S"
  end

  # PRIME achieves the ideal reconstruction workload when failures ==
  # protection.  The paper did not consider other cases
  def reconstruction_workload(failures)
    (@stripesize - 1.0) / (@clustsize - 1.0)
  end

  def parallel_read_count(chunks)
    (chunks.to_f / @clustsize).ceil + 1
  end

  def userloc(chunkaddr)
    [disk(chunkaddr), offset(chunkaddr)]
  end

  def checkloc(chunkaddr, checkidx)
    [checkdisk(chunkaddr, checkidx), checkoffset(chunkaddr, checkidx)]
  end

  def invloc(disk, offset)
    k = @stripesize
    n = @clustsize
    f = @protection
    z = offset / k
    y = z % (n - 1) + 1

    # Algorithm:
    # Calculate the minimum and maximum possible addresses, assuming this is a
    # data chunk.  Also calculate the addresses of the check chunks that are
    # mapped to this iteration of this disk.  Virtually merge the two lists and
    # walk through them until the correct chunk is found.  Time complexity is
    # O(f) plus the time to sort a list of f elements.
    
    amin = [n * (offset - f * (z + 1)), @m * n * z].max
    # Round up to next multiple of n, mod disk * y^-1
    amin += (disk * invmod(y, n) - amin) % n
    amax = n * (offset + 1 - f * z)
    raise StandardError.new("Assertion Error") if disk(amin) != disk

    a = amin
    # possible_check_chunks is an array of [address, checkidx] corresponding to
    # the check chunks on this disk at this iteration
    possible_check_chunks = (0 .. f - 1).map do |j|
      eff_stripe = ((disk * invmod(y, n) - j) * invmod(@m, n) - 1) % n
      [@m * eff_stripe + @m * n * z, j]
    end.sort

    check_chunk_idx = 0
    loop do
      raise StandardError.new("Assertion error") if a >= amax
      cc = possible_check_chunks[check_chunk_idx]
      if cc.nil? || a < cc[0]
        if ((a - @m * n * z) / n).floor + check_chunk_idx + k * z == offset
          return [a, false, 0]
        else
          a += n
        end
      else
        raise StandardError.new("Assertion error") if cc.nil? || a == cc[0]
        if ((a - @m * n * z) / n).floor + check_chunk_idx + k * z == offset
          return [cc[0], true, cc[1]]
        else
          check_chunk_idx += 1
        end
      end
    end
  end

  private
  def disk(a)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    y = (z % (@clustsize - 1)) + 1  # the stride
    (a * y) % @clustsize
  end

  def offset(a)
    n = @clustsize
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    y = (z % (@clustsize - 1)) + 1  # the stride
    ((a - @m * n * z).to_f / n).floor + (0..@protection-1).map do |j|
      cb_stripe = ((disk(a) * invmod(y, n) - j) * invmod(@m, n) - 1) % n
      (a / @m).floor % n > cb_stripe ? 1 : 0
    end.reduce(:+) + @stripesize * z
  end

  def checkdisk(a, i)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    y = (z % (@clustsize - 1)) + 1  # the stride
    ((((a / @m).floor + 1)*@m + i) * y) % @clustsize
  end

  def checkoffset(a, i)
    n = @clustsize
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    y = (z % (@clustsize - 1)) + 1  # the stride
    eff_a = (((a / @m).floor + 1) * @m + i)
    ((eff_a - @m * n * z).to_f / n).floor + (0..@protection-1).map do |j|
      cb_stripe = ((checkdisk(a, i) * invmod(y, n) - j) * invmod(@m, n) - 1) % n
      (a / @m).floor % n > cb_stripe ? 1 : 0
    end.reduce(:+) + @stripesize * z
  end
end

class Relpr < Layout
  def initialize(clustsize, stripesize, protection)
    super(clustsize, stripesize, protection)
    @m = stripesize - protection
    @totient_n = totient(clustsize)
    @strides = []
    (1 .. clustsize - 1).each do |y|
      @strides << y if y.gcd(@clustsize) == 1
    end
    if @strides.size != @totient_n
      raise StandardError.new("Assertion error")
    end
    @g = @clustsize.gcd @m
  end

  def depth
    @stripesize * totient(@clustsize)
  end

  def name
    "RELPR"
  end

  # Upper bound for failures == 1.  Most constructions will do better
  def reconstruction_workload(failures)
    (@stripesize - 1.0) / totient(@clustsize)
  end

  def parallel_read_count(chunks)
    (chunks.to_f / @clustsize).ceil + 1
  end

  def userloc(chunkaddr)
    [disk(chunkaddr), offset(chunkaddr)]
  end

  def checkloc(chunkaddr, checkidx)
    [checkdisk(chunkaddr, checkidx), checkoffset(chunkaddr, checkidx)]
  end

  private
  def disk(a)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    l = z % @totient_n
    yl = @strides[l]
    j = (a / (@m * (@clustsize / @g))).floor % @g
    ((a + j) * yl) % @clustsize
  end

  def offset(a)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    (a / @clustsize).floor + @protection * z
  end

  def checkdisk(a, i)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    l = z % @totient_n
    yl = @strides[l]
    j = (a / (@m * (@clustsize / @g))).floor % @g
    ((((a / @m).floor + 1)*@m + i + j) * yl) % @clustsize
  end

  def checkoffset(a, i)
    z = (a.to_f / (@m * @clustsize)).floor  # the iteration number
    (z + 1)*@stripesize - @protection + i
  end
end

class PPL0 < Layout
  def initialize(clustsize, stripesize, protection)
    unless protection == 1
      raise NotImplementableError.new
    end
    factors = clustsize.prime_division
    unless factors.size == 1
      raise NotImplementableError.new
    end
    @p = factors.first.first
    @n = factors.first[1]
    kfactors = (stripesize - 1).prime_division
    unless kfactors.size == 1
      raise NotImplementableError.new
    end
    @m = kfactors.first[1]
    unless kfactors.first.first == factors.first.first
      raise NotImplementableError.new
    end
    @g = (0..@p**@m - 1).map do |i|
      finv(i * @p**(@n - @m))
    end
    @g << finv(@p**@n - 1)  # The arbitrarily chosen parity element
    super(clustsize, stripesize, protection)
  end

  def depth
    @stripesize * (@clustsize - 1)
  end

  def name
    "PPL0"
  end

  # PPL0 achieves the ideal reconstruction workload, when failures ==
  # protection == 1
  def reconstruction_workload(failures)
    (@stripesize - 1.0) / (@clustsize - 1.0)
  end

  def parallel_read_count(chunks)
    (@p.to_f * chunks / (@clustsize * (@p / 2).floor.to_f)).ceil + 1
  end

  def userloc(a)
    v = @clustsize
    k = @stripesize
    yprime = (a / (v * (k - 1))).floor + 1
    aprime = a % (v * (k - 1))
    y = finv(yprime)
    xprime = (aprime / (k-1)).floor
    aprimeprime = aprime % (k - 1)
    x = finv(xprime)
    d = y * (x + @g[aprimeprime])
    dprime = d.substitute(@p)
    o = (yprime - 1) * k + (xprime / (v / (k - 1))).floor
    [dprime, o]
  end

  def checkloc(a, i)
    v = @clustsize
    k = @stripesize
    yprime = (a / (v * (k - 1))).floor + 1
    aprime = a % (v * (k - 1))
    y = finv(yprime)
    xprime = (aprime / (k-1)).floor
    aprimeprime = aprime % (k - 1)
    x = finv(xprime)
    d = y * (x + @g[-1])
    dprime = d.substitute(@p)
    o = yprime * k - 1
    [dprime, o]
  end

  private
  # Returns a GFPolynomial which, evaluated at @p, returns s
  def finv(s)
    coeffs = [0] * (@n-1)
    (@n-1).downto(0).each do |b|
      a = s / @p**b
      s -= a * @p**b
      coeffs[b] = a
    end
    GFPolynomial.new(@p, @n, coeffs)
  end
end

# Randomized Permutation Layout, with gamma = 0.5
class RPL < Layout
  def initialize(clustsize, stripesize, protection)
    unless protection == 1
      raise NotImplementableError.new
    end
    super(clustsize, stripesize, protection)
    @gamma = 0.5
  end

  # RPL depth is dependent on the precise construction
  # TODO: construct RPL layout with given parameters and measure depth
  def depth
    "?"
  end

  def name
    "RPL"
  end

  def reconstruction_workload(failures)
    (1 + @gamma) * (@stripesize - 1.0) / (@clustsize - 1.0)
  end

  def parallel_read_count(chunks)
    (chunks.to_f / @clustsize).ceil + 1
  end
end

class Auto
  def self.new(clustsize, stripesize, protection)
    layout = nil
    [PrimeLayout, PPL0, Relpr, RPL].each do |klass|
      begin
        layout = klass.new(clustsize, stripesize, protection)
        break
      rescue NotImplementableError
      end
    end
    layout
  end
end

# This class uses a Layout object to explicitly construct the map of a full
# iteration of the layout
class ChunkMap
  def initialize(layout)
    @layout = layout

    @stripes = NArray.int(layout.clustsize, layout.depth)
    @offsets = NArray.byte(layout.clustsize, layout.depth)
    @datachunks = NArray.byte(layout.clustsize, layout.depth)
    @checkchunks = NArray.byte(layout.clustsize, layout.depth)
    (0 .. layout.size / layout.stripesize - 1).each do |stripe|
      (0 .. layout.stripesize - layout.protection - 1).each do |stripeofs|
        useraddr = stripe * (layout.stripesize - layout.protection) + stripeofs
        (disk, offset) = layout.userloc(useraddr)
        begin
          @stripes[disk, offset] = stripe
          @offsets[disk, offset] = stripeofs
          @datachunks[disk, offset] = 1
        rescue NoMethodError
          raise StandardError.new("mapped disk is out of bounds")
        end
        begin
          (revaddr, ischeck, revidx) = layout.invloc(disk, offset)
          if revaddr != useraddr || ischeck
            raise StandardError.new("Reverse mapping failed for user addr #{useraddr}")
          end
        rescue NotImplementedError
        end
      end
      (0 .. layout.protection - 1).each do |checkidx|
        useraddr = stripe * (layout.stripesize - layout.protection)
        (disk, offset) = layout.checkloc(useraddr, checkidx)
        begin
          @stripes[disk, offset] = stripe
          @offsets[disk, offset] = checkidx
          @checkchunks[disk, offset] = 1
        rescue NoMethodError
          raise StandardError.new("mapped disk is out of bounds")
        end
        begin
          (revaddr, ischeck, revidx) = layout.invloc(disk, offset)
          if revaddr != useraddr || ! ischeck || revidx != checkidx
            raise StandardError.new("Reverse mapping failed for check block (#{useraddr}, #{checkidx}")
          end
        rescue NotImplementedError
        end
      end
    end
  end

  # Does each disk have no more than one chunk from each stripe?
  def fault_tolerant?
    (0 .. @layout.clustsize - 1).each do |col|
      stripes = (0 .. @layout.depth - 1).map do |row|
        @stripes[col, row]
      end.to_set
      if stripes.size < @layout.depth
        return false
      end
    end
    true
  end

  def max_parity
    max_parity = 0
    (0 .. @layout.clustsize - 1).each do |col|
      nparity = 0
      (0 .. @layout.depth - 1).each do |row|
        nparity += 1 if @datachunks[col, row] == 0
      end
      max_parity = [max_parity, nparity].max
    end
    max_parity.to_f / @layout.depth
  end

  # Is the transform monotonic?  That is, are monotonically increasing logical
  # accesses guaranteed to result in monotonically increasing physical
  # accesses?  This is very important for shingled drives.
  def monotonic?
    d = @layout.depth
    (0 .. @layout.clustsize - 1).map do |col|
      (@stripes[col, 1..d-1] - @stripes[col, 0..d-2]).gt(0).all?
    end.all?
  end

  # Maximum number of chunks that must be read from any one disk in order to
  # service a user read of the given number of chunks
  def parallel_read_count(chunks)
    uc = @layout.userchunks
    user2disk = NArray.int(uc)
    user2disk[] = (0 .. uc - 1).map do |chunk|
      @layout.userloc(chunk)[0]
    end
    (0 .. uc - chunks).map do |start|
      counts = NArray.int(@layout.clustsize)
      to_read = user2disk[start .. start + chunks - 1]
      to_read.each do |disk|
        counts[disk] += 1
      end
      counts.max
    end.max
  end

  # Returns the max, for all pairs of disks, of the fraction of each disk's
  # units that must be read to reconstruct a failure of failures disks
  def reconstruction_workload(failures)
    m = @layout.stripesize - @layout.protection
    (0 .. @layout.clustsize - 1).to_a.combination(failures).map do |failed|
      # Failed is an array containing the indices of failed columns

      # number of chunks that must be read from each disk
      disk_reads = NArray.int(@layout.clustsize)

      # set of stripes, indicating which have any amount of damage
      damaged_stripes = Bitset.new(@layout.stripes)
      failed.each do |col|
        damaged_stripes.set(*@stripes[col, true])
      end

      damaged_stripes.each_with_index do |damaged, stripe|
        next unless damaged

        # Need to read the lowest undamaged @m chunks from stripe
        read_count = 0
        # Read all undamaged data chunks
        (0 .. m - 1).each do |i|
          disk, offset = @layout.userloc(stripe * m + i)
          next if failed.include? disk
          disk_reads[disk] += 1
          read_count += 1
        end
        # Read sufficiently many undamaged parity chunks
        (0 .. @layout.protection - 1).each do |i|
          break if read_count >= m
          disk, offset = @layout.checkloc(stripe * m, i)
          next if failed.include? disk
          disk_reads[disk] += 1
          read_count += 1
        end
      end

      disk_reads.max
    end.max.to_f / @layout.depth
  end

  def to_s
    sw = Math.log(@layout.stripes - 1, 10).ceil
    ow = Math.log([1, @layout.stripesize - @layout.protection - 1].max, 10).ceil
    tw = sw + ow + 3
    s = (0 .. @stripes.shape[1] - 1).map do |row|
      (0 .. @stripes.shape[0] - 1).map do |col|
        code = if @datachunks[col, row] == 1
                 "D"
               else
                 "C"
               end
        format("%-#{tw}s", "#{code}#{@stripes[col, row]}.#{@offsets[col, row]}")
      end.join(" ")
    end.join("\n")
    s
  end
end

def parseargs(argv)
  options = OpenStruct.new
  options.layout = "auto"
  options.verbose = false

  parser = OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} [options] <CLUSTER SIZE> <STRIPE SIZE> <REDUNDANCY LEVEL>"
    opts.on("--layout LAYOUT", LAYOUTS, "Select layout",
            "  (#{LAYOUTS})") do |layout|
      options.layout = layout
    end

    opts.on("-v", "--verbose", "Verbose output") do
      options.verbose = true
    end
  end

  parser.parse!(argv)
  if argv.size != 3
    puts parser
    exit(2)
  end
  options.clustsize = argv[0].to_i
  options.stripesize = argv[1].to_i
  options.protection = argv[2].to_i

  return options
end


def main(argv)
  options = parseargs(argv)
  klass = case options.layout
  when "auto"
    Auto
  when "classic"
    Classic
  when "prime"
    PrimeLayout
  when "primes"
    PrimeS
  when "relpr"
    Relpr
  when "rpl"
    RPL
  when "ppl0"
    PPL0
  end
  layout = klass.new(options.clustsize, options.stripesize, options.protection)
  ideal = Ideal.new(options.clustsize, options.stripesize, options.protection)

  code = "#{options.clustsize},#{options.stripesize},#{options.protection}"

  begin
    cm = ChunkMap.new(layout)
    if options.verbose
      puts cm
    end
    mp = cm.max_parity
    arw = (1 .. layout.protection).map do |failures|
      cm.reconstruction_workload failures
    end
    fault_tolerant = cm.fault_tolerant? ? "Yes" : "No"
    aprc = cm.parallel_read_count(options.clustsize)
    actual = true
  rescue NotImplementedError
    actual = false
  end

  imp = ideal.max_parity
  rw = layout.reconstruction_workload 1
  irw = (1 .. layout.protection).map do |failures|
    ideal.reconstruction_workload failures
  end
  prc = layout.parallel_read_count(options.clustsize)
  iprc = ideal.parallel_read_count(options.clustsize)
  name = "#{layout.name}-#{code}"
  colw = 40
  puts format("%-#{colw}s %-20s%-19s", "Layout:", name, "(relative to ideal)")
  if actual
    puts format("%-#{colw}s %-20s", "Fault tolerant?", fault_tolerant)
    puts format("%-#{colw}s %-20s%-19s", "Max Parity Overhead:", mp, mp / imp)
  end
  puts format("%-#{colw}s %-20s%-19s", "Max Reconstruction Workload bound:",
              rw, rw / irw[0])
  if actual
    (1 .. layout.protection).each do |failures|
      idx = failures - 1
      puts format("Max Reconstruction Workload, %d failures: %-20s%-19s",
                  failures, arw[idx], arw[idx] / irw[idx])
    end
    puts format("%-#{colw}s %-20s", "Monotonic:", cm.monotonic?)
  end
  puts format("%-#{colw}s %-20s%-19s", "Parallel Read Count bound, one row:",
              prc, prc.to_f / iprc)
  if actual
    puts format("%-#{colw}s %-20s%-19s", "Actual Parallel Read Count, 1 row:",
                aprc, aprc.to_f / iprc)
  end
  puts format("%-#{colw}s %d rows", "Depth:", layout.depth)
  0
end

exit(main(ARGV)) if __FILE__ == $0
