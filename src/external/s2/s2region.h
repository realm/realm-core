// Copyright 2005 Google Inc. All Rights Reserved.

#ifndef UTIL_GEOMETRY_S2REGION_H_
#define UTIL_GEOMETRY_S2REGION_H_

#include "s2.h"

class S2Cap;
class S2Cell;
class S2LatLngRect;

// An S2Region represents a two-dimensional region over the unit sphere.
// It is an abstract interface with various concrete subtypes.
//
// The main purpose of this interface is to allow complex regions to be
// approximated as simpler regions.  So rather than having a wide variety
// of virtual methods that are implemented by all subtypes, the interface
// is restricted to methods that are useful for computing approximations.
class S2Region {
 public:
  virtual ~S2Region();

  // Return a deep copy of this region.  If you want to narrow the result to a
  // specific known region type, use down_cast<T*> from basictypes.h.
  // Subtypes return pointers to that subtype from their Clone() methods.
  virtual S2Region* Clone() const = 0;

  // Return a bounding spherical cap. This is not guaranteed to be exact.
  virtual S2Cap GetCapBound() const = 0;

  // Return a bounding latitude-longitude rectangle that contains the region.
  // The bounds are not guaranteed to be tight.
  virtual S2LatLngRect GetRectBound() const = 0;

  // If this method returns true, the region completely contains the given
  // cell.  Otherwise, either the region does not contain the cell or the
  // containment relationship could not be determined.
  virtual bool Contains(S2Cell const& cell) const = 0;

  // If this method returns false, the region does not intersect the given
  // cell.  Otherwise, either region intersects the cell, or the intersection
  // relationship could not be determined.
  virtual bool MayIntersect(S2Cell const& cell) const = 0;

  // Return true if and only if the given point is contained by the region.
  // The point 'p' is generally required to be unit length, although some
  // subtypes may relax this restriction.
  // NOTE: If you will be calling this function on one specific subtype only,
  // or if performance is a consideration, please use the non-virtual
  // method Contains(S2Point const& p) declared below!
  virtual bool VirtualContainsPoint(S2Point const& p) const = 0;

  /////////////////////////////////////////////////////////////////////////
  // The following are NON-VIRTUAL methods (for efficiency reasons) that
  // happen to be implemented by all subclasses.  You cannot call these
  // methods unless you have an object of a particular subtype.
  //
  // bool Contains(S2Point const& p) const;
  //
  // Return true if and only if the given point is contained by the region.
  // The point 'p' is generally required to be unit length, although some
  // subtypes may relax this restriction.
};

#endif  // UTIL_GEOMETRY_S2REGION_H_
