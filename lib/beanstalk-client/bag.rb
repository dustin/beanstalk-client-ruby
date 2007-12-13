# beanstalk-client/bag.rb - client library for beanstalk

# Copyright (C) 2007 Philotic Inc.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

class Beanstalk::Bag
  def initialize(initial_size=0, &default)
    @default = default
    @items = []
    initial_size.times{give(default.call())}
  end

  def give(x)
    (@items << x)[-1]
  end

  def take()
    @items.pop or @default.call()
  end

  def size()
    @items.size
  end
end
