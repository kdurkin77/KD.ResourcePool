namespace KD.ResourcePool

open System
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks

open Microsoft.Extensions.Options


type private PoolMessage<'T> =
    | TryGetResource of AsyncReplyChannel<Result<'T option, exn>>
    | ReturnResource of 'T
    | Shutdown


type Resource<'T> = {
    Id  : Guid
    R   : 'T
    }


type private State<'T> = {
    InUseCount          : int
    ShutdownRequested   : bool
    Resources           : Resource<'T> list
    }


[<CLIMutable>]
type ResourcePoolOptions<'T> = {
    MaxResources                : int
    AllowCreation               : bool
    InitialResources            : 'T seq
    Create                      : Func<Task<Resource<'T>>>
    Destroy                     : Action<Resource<'T>>
    DoesResourceNeedDestroyed   : Func<Resource<'T>, Task<bool>>
    WaitForResourceDelay        : TimeSpan
    }


[<Sealed>]
type ResourcePool<'T>(options: ResourcePoolOptions<'T> IOptions) =

    let opts                        = 
        if isNull options 
        then nullArg "options" 
        else options.Value
    let maxResources                = 
        if opts.MaxResources <= 0 
        then invalidArg "options.MaxResources" (opts.MaxResources.ToString()) 
        else opts.MaxResources
    let allowCreation               = 
        opts.AllowCreation
    let create                      = 
        if allowCreation && isNull opts.Create 
        then nullArg "options.Create" 
        else opts.Create
    let initialResources            = 
        if isNull opts.InitialResources 
        then [] 
        else opts.InitialResources |> Seq.map(fun r -> { Id = Guid.NewGuid(); R = r; }) |> Seq.toList
    let destroy                     = 
        if isNull opts.Destroy 
        then nullArg "options.Destroy" 
        else opts.Destroy
    let doesResourceNeedDestroyed   = 
        if isNull opts.DoesResourceNeedDestroyed 
        then nullArg "options.DoesResourceNeedDestroyed" 
        else opts.DoesResourceNeedDestroyed
    let waitForResourceDelay        = 
        if opts.WaitForResourceDelay = TimeSpan.Zero 
        then invalidArg "options.WaitForResourceDelay" "WaitForResourceDelay must be non-zero"
        else opts.WaitForResourceDelay

    do if (not allowCreation) && maxResources <> initialResources.Length then 
        failwith "Resources initialized does not equal max"

    let mutable disposed = false

    let destroy r =
        try destroy.Invoke(r)
        with _ -> ()

    let doesResourceNeedDestroyed r =
        try doesResourceNeedDestroyed.Invoke(r)
        with _ -> Task.FromResult false

    let _agent = MailboxProcessor.Start(fun inbox ->
        let rec nextMessage (state: 'T State) = async {
            match! inbox.Receive() with
            |Shutdown when state.ShutdownRequested ->
                return! nextMessage state

            | Shutdown ->
                state.Resources |> List.iter destroy
                return! nextMessage { state with
                                        Resources = []
                                        ShutdownRequested = true
                                        }

            | ReturnResource r when state.ShutdownRequested ->
                destroy r
                return! nextMessage { state with InUseCount = state.InUseCount - 1 }

            | ReturnResource r ->
                return! nextMessage { state with 
                                        InUseCount = state.InUseCount - 1
                                        Resources  = r :: state.Resources
                                        }

            | TryGetResource channel when state.ShutdownRequested ->
                channel.Reply (try invalidOp "Resource Pool Is Shutting Down" with ex -> Error ex)
                return! nextMessage state

            | TryGetResource channel when state.InUseCount >= maxResources ->
                channel.Reply (Ok None)
                return! nextMessage state

            | TryGetResource channel ->
                let! r', resources, inUseCount = async {
                    match state.Resources with
                    | [] ->
                        try
                            if not allowCreation then failwith "Creation not allowed"
                            let! r = create.Invoke() |> Async.AwaitTask
                            return Ok (Some r), [], (state.InUseCount + 1)
                        with ex ->
                            return Error ex, [], state.InUseCount
                    | r :: xs ->
                        match! doesResourceNeedDestroyed r |> Async.AwaitTask with
                        | true ->
                            destroy r
                            return Ok None, xs, state.InUseCount
                        | false ->
                            return Ok (Some r), xs, (state.InUseCount + 1)
                    }

                channel.Reply r'
                return! nextMessage { state with
                                        InUseCount = inUseCount
                                        Resources  = resources
                                        }
            }

        nextMessage { InUseCount = 0; Resources = initialResources; ShutdownRequested = false }
        )


    member _.Use (action: Func<'T, CancellationToken, Task<'TResult>>, cancellationToken: CancellationToken) =
        if isNull action then nullArg "action"

        let rec waitForResource () = async {
            match! _agent.PostAndAsyncReply(TryGetResource) with
            | Ok None -> 
                do! Async.Sleep waitForResourceDelay
                cancellationToken.ThrowIfCancellationRequested()
                return! waitForResource()

            | Ok (Some resource) ->
                try
                    return! action.Invoke(resource.R, cancellationToken) |> Async.AwaitTaskAndUnwrapEx
                finally
                    _agent.Post(ReturnResource resource)

            | Error exn ->
                ExceptionDispatchInfo.Capture(exn).Throw()
                return Unchecked.defaultof<_>
            }

        waitForResource() |> Async.StartAsTask


    interface IDisposable with
        member this.Dispose() =
            if not disposed then
                _agent.Post(Shutdown)
                disposed <- true
                GC.SuppressFinalize(this)
