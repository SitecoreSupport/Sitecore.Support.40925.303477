using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using System.Web;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Publishing.ManifestCalculation;

namespace Sitecore.Support.Framework.Publishing.ManifestCalculation
{
  public class VariantsValidationTargetProducer : Sitecore.Framework.Publishing.ManifestCalculation.VariantsValidationTargetProducer
  {
    public VariantsValidationTargetProducer(IObservable<CandidateValidationContext> publishStream, Sitecore.Framework.Publishing.ManifestCalculation.ITargetItemIndexService targetIndexService, Sitecore.Framework.Publishing.Item.Language[] languages, Guid targetId, int bufferSize, bool republishAllVariants, DateTime publishTimestamp, CancellationTokenSource errorSource, Microsoft.Extensions.Logging.ILogger logger, Microsoft.Extensions.Logging.ILogger diagnosticLogger) : base(publishStream, targetIndexService, languages, targetId, bufferSize, republishAllVariants, publishTimestamp, errorSource, logger, diagnosticLogger)
    {
    }
    protected override void Initialize()
    {
      _publishStream
          .ObserveOn(Scheduler.Default)
          .Buffer(_bufferSize)
          .Subscribe(ctxs =>
          {
            try
            {
              _logger.LogTrace("Loading steps ..");

                    // immediately emit known invalid candidates
                    Emit(ctxs
                        .Where(ctx => !ctx.IsValid)
                        .Select(ctx => new InvalidCandidateTargetContext(_targetId, ctx.Id))
                        .ToArray());

              var candidates = ctxs
                        .Where(ctx => ctx.IsValid)
                        .Select(ctx => ctx.AsValid().Candidate)
                        .Distinct(new SupportComparer())
                        .ToArray();

              if (!candidates.Any()) return;

                    // get all published variants for all candidates in the batch
                    var currentPublishedItemsResult = _targetIndexService.GetItemMetadatas(candidates.Select(c => c.Id).ToArray()).Result;
              var candidatesTargetIndex = currentPublishedItemsResult.ToLookup(r => r.Id);

              foreach (var candidate in candidates)
              {
                _errorToken.ThrowIfCancellationRequested();

                      // get published variants for the current cadidate
                      var publishedCandidateItem = candidatesTargetIndex[candidate.Id].FirstOrDefault() ??
                          new ItemDoesntExistMetadata(candidate.Id);

                var candidateContext = new CandidatePromotionContext(candidate, publishedCandidateItem);

                ProcessCandidate(candidateContext);
              }
            }
            catch (OperationCanceledException)
            {
              if (_errorToken.IsCancellationRequested)
              {
                _logger.LogTrace("VariantsOperationsProducer cancelled.");
                Completed();
              }
            }
            catch (Exception ex)
            {
              _logger.LogError(0, ex, $"Error in the {nameof(VariantsValidationTargetProducer)}");

              Errored(ex);
            }
          },
          ex =>
          {
            Errored(ex);
          },
          () =>
          {
            _logger.LogTrace($"{this.GetType().Name} completed.");
            Completed();
          },
          _errorToken);
    }
  }
}