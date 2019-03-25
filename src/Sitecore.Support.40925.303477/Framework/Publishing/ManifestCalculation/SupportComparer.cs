using Sitecore.Framework.Publishing.ManifestCalculation;
using System.Collections.Generic;

namespace Sitecore.Support.Framework.Publishing.ManifestCalculation
{
  public class SupportComparer : IEqualityComparer<IPublishCandidate>
  {
    public bool Equals(IPublishCandidate x, IPublishCandidate y)
    {
      return x != null &&
          y != null &&
          x.Id.Equals(y.Id);
    }

    public int GetHashCode(IPublishCandidate obj)
    {
      unchecked
      {
        return obj.Id.GetHashCode();
      }
    }
  }
}