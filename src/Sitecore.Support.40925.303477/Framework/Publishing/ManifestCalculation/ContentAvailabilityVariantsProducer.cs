using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.ManifestCalculation;

namespace Sitecore.Support.Framework.Publishing.ManifestCalculation
{
  public class ContentAvailabilityVariantsProducer : Sitecore.Support.Framework.Publishing.ManifestCalculation.VariantsValidationTargetProducer
  {
    public ContentAvailabilityVariantsProducer(IObservable<CandidateValidationContext> publishStream, ITargetItemIndexService targetIndexService, Language[] languages, Guid targetId, int bufferSize, bool republishAllVariants, DateTime publishTimestamp, CancellationTokenSource errorSource, ILogger logger, ILogger diagnosticLogger) : base(publishStream, targetIndexService, languages, targetId, bufferSize, republishAllVariants, publishTimestamp, errorSource, logger, diagnosticLogger)
    {
    }

    protected override IEnumerable<IPublishCandidateVariant> ChoosePublishableVariantsForLang(IPublishCandidate candidate, string language,
        IPublishCandidateVariant[] candidateVariantsForLang)
    {
      return candidateVariantsForLang;
    }
  }
}