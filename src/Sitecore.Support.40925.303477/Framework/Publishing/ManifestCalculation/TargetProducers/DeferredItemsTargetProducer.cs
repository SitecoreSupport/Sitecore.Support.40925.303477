using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Conditions;
using Sitecore.Framework.Publishing.Data;
using Sitecore.Framework.Publishing.Item;
using Sitecore.Framework.Publishing.ItemIndex;
using Sitecore.Framework.Publishing.ManifestCalculation;

namespace Sitecore.Support.Framework.Publishing.ManifestCalculation.TargetProducers
{
  public class DeferredItemsTargetProducer : ProducerBase<CandidateValidationTargetContext>
  {
    private readonly IObservable<CandidateValidationTargetContext> _publishStream;
    private readonly ISourceIndexWrapper _sourceIndex;
    private readonly ITargetItemIndexService _targetIndex;
    private readonly ILogger _logger;
    private readonly CancellationToken _errorToken;

    // parent id, list of the valid contexts
    private readonly Dictionary<Guid, List<CandidateValidationTargetContext>> _deferredCandidates = new Dictionary<Guid, List<CandidateValidationTargetContext>>();

    // item id, context
    private readonly Dictionary<Guid, CandidateValidationTargetContext> _validsToEmit = new Dictionary<Guid, CandidateValidationTargetContext>();

    private readonly HashSet<Guid> _invalidsCandidateIds = new HashSet<Guid>();

    private readonly List<Guid> _emittedItemsIds = new List<Guid>();

    public DeferredItemsTargetProducer(
        IObservable<CandidateValidationTargetContext> publishStream,
        ISourceIndexWrapper sourceIndex,
        ITargetItemIndexService targetIndex,
        CancellationTokenSource errorSource,
        ILogger logger)
    {
      Condition.Requires(publishStream, nameof(publishStream)).IsNotNull();
      Condition.Requires(sourceIndex, nameof(sourceIndex)).IsNotNull();
      Condition.Requires(targetIndex, nameof(targetIndex)).IsNotNull();
      Condition.Requires(errorSource, nameof(errorSource)).IsNotNull();
      Condition.Requires(logger, nameof(logger)).IsNotNull();

      _publishStream = publishStream;
      _sourceIndex = sourceIndex;
      _targetIndex = targetIndex;
      _logger = logger;
      _errorToken = errorSource.Token;

      Initialize();
    }

    private void Initialize()
    {
      _publishStream
          .ObserveOn(Scheduler.Default)
          .Subscribe(ctx =>
          {
            try
            {
              // root item is always present in target database and shouldn't be re-published
              if (ctx.Id.Equals(Guid.Parse("{11111111-1111-1111-1111-111111111111}")))
              {
                return;
              }

              // Make sure items with no parent ID are treated as invalid items.
              if (!ctx.IsValid || !ctx.AsValid().Candidate.ParentId.HasValue)
              {
                if (!_invalidsCandidateIds.Contains(ctx.Id))
                {
                  _invalidsCandidateIds.Add(ctx.Id);
                }
                Emit(ctx);
              }
              else
              {
                var validContext = ctx.AsValid();

                if (_targetIndex.ItemExists(validContext.Candidate.ParentId.Value).Result)
                {
                  _validsToEmit.Add(validContext.Id, validContext);
                  _emittedItemsIds.Add(validContext.Id); // keep track of all the item ids will be created in the target db
                        ProcessDeferredDescendants(validContext.Id); // process any defered children for that item
                      }
                else // parent might be in the same publish job
                      {
                  var parentId = validContext.Candidate.ParentId.Value;
                  if (_deferredCandidates.TryGetValue(parentId, out var list))
                  {
                    list.Add(validContext);
                  }
                  else
                  {
                    list = new List<CandidateValidationTargetContext> { validContext };
                    _deferredCandidates.Add(parentId, list);
                  }
                }
              }

            }
            catch (OperationCanceledException exception)
            {
              _logger.LogWarning(new EventId(), exception, "DeferredItemsTargetProducer cancelled.");
              Completed();
            }
            catch (Exception ex)
            {
              _logger.LogError(0, ex, $"Error in the {nameof(DeferredItemsTargetProducer)}");
              Errored(ex);
              throw;
            }
          },
              ex =>
              {
                base.Errored(ex);
              },
              () =>
              {
                Flush();
                base.Completed();
              },
              _errorToken);
    }

    private void ProcessDeferredDescendants(params Guid[] validParentIds)
    {
      while (true)
      {
        var deferredChildren = validParentIds.SelectMany(x =>
        {
          if (_deferredCandidates.TryGetValue(x, out var list))
          {
            _deferredCandidates.Remove(x);
            return list.AsEnumerable();
          }
          return Enumerable.Empty<CandidateValidationTargetContext>();
        }).ToArray();

        if (deferredChildren.Any())
        {
          //Emit(deferredChildren);
          deferredChildren.ForEach( x =>
          {
            _validsToEmit.Add(x.Id, x);
            _emittedItemsIds.Add(x.Id);
          });
          validParentIds = deferredChildren
              .Select(x => x.Id).ToArray();
        }
        else
        {
          break;
        }
      }
    }

    private void Flush()
    {
      if (_emittedItemsIds.Any())
      {
        // flush any raiming deferred items
        ProcessDeferredDescendants(_emittedItemsIds.ToArray());
      }

      var invalidDescendantsList = _invalidsCandidateIds
          .SelectMany(x => _sourceIndex.GetDescendants(x).Result).Distinct()
          .Select(x => x.Descriptor.Id);

      var invalidDescendantsLookup = new HashSet<Guid>(invalidDescendantsList);

      // don't emit any item that appears in the invalid descendants list
      foreach (var item in _validsToEmit)
      {
        if (invalidDescendantsLookup.Contains(item.Key))
        {
          // if item exists on target emit invalid, otherwise ignore it
          if (_targetIndex.ItemExists(item.Key).Result)
          {
            Emit(new InvalidCandidateTargetContext(item.Value.TargetId, item.Value.Id));
          }
        }
        else
        {
          Emit(item.Value);
        }
      }
    }
  }
}