using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Conditions;
using Sitecore.Framework.Publishing.ContentTesting;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.ManifestCalculation;

namespace Sitecore.Support.Framework.Publishing.ManifestCalculation
{
  public class TestingVariantsValidationTargetProducer : Sitecore.Support.Framework.Publishing.ManifestCalculation.VariantsValidationTargetProducer
  {
    protected readonly ITestableContentRepository _contentTestRepository;
    protected Dictionary<Guid, HashSet<IItemVariantIdentifier>> _itemsUnderTest;

    protected static readonly IItemVariantIdentifierComparer _testingLocatorComparer = new IItemVariantIdentifierComparer();

    public TestingVariantsValidationTargetProducer(
        IObservable<CandidateValidationContext> publishStream,
        ITargetItemIndexService targetIndexService,
        ITestableContentRepository contentTestRepository,
        Language[] languages,
        Guid targetId,
        int bufferSize,
        bool republishAllVariants,
        DateTime publishTimestamp,
        CancellationTokenSource errorSource,
        ILogger logger,
        ILogger diagnosticLogger) : base(publishStream, targetIndexService, languages, targetId, bufferSize, republishAllVariants, publishTimestamp, errorSource, logger, diagnosticLogger)
    {
      Condition.Requires(contentTestRepository, nameof(contentTestRepository)).IsNotNull();

      _contentTestRepository = contentTestRepository;
    }

    protected override IEnumerable<IPublishCandidateVariant> ChoosePublishableVariantsForLang(
        IPublishCandidate candidate,
        string language,
        IPublishCandidateVariant[] candidateVariantsForLang)
    {
      if (_itemsUnderTest == null)
      {
        LoadTestData().Wait();
      }

      // is the current item under test
      HashSet<IItemVariantIdentifier> variantsUnderTest;
      if (_itemsUnderTest.TryGetValue(candidate.Id, out variantsUnderTest))
      {
        // is the current language under test...
        if (variantsUnderTest.Any(v => v.Language == language))
        {
          // if so, filter down to only variants that are under test (not just the latest one)
          return candidateVariantsForLang.Where(v => variantsUnderTest.Contains(v.Variant));
        }
      }

      return base.ChoosePublishableVariantsForLang(candidate, language, candidateVariantsForLang);
    }

    protected virtual async Task LoadTestData()
    {
      _itemsUnderTest = (await _contentTestRepository.GetAllVariantsUnderTest().ConfigureAwait(false))
          .GroupBy(v => v.Id)
          .ToDictionary(g => g.Key, g => new HashSet<IItemVariantIdentifier>(g, _testingLocatorComparer));
    }
  }
}