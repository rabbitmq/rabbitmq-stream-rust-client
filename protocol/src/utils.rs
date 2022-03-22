pub trait TupleMapperFirst<Input, Output, Result>
where
    Self: Sized,
{
    fn map_first(self, mapper: impl Fn(Input) -> Output) -> Result;
}

pub trait TupleMapperSecond<Input, Output, Result>
where
    Self: Sized,
{
    fn map_second(self, mapper: impl FnOnce(Input) -> Output) -> Result;
}

impl<S, E, I, O> TupleMapperFirst<I, O, Result<(O, S), E>> for Result<(I, S), E> {
    fn map_first(self, mapper: impl Fn(I) -> O) -> Result<(O, S), E> {
        self.map(|(first, second)| (mapper(first), second))
    }
}

impl<S, E, I, O> TupleMapperSecond<I, O, Result<(S, O), E>> for Result<(S, I), E> {
    fn map_second(self, mapper: impl FnOnce(I) -> O) -> Result<(S, O), E> {
        self.map(|(first, second)| (first, mapper(second)))
    }
}
