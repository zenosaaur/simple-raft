use winnow::{
    Parser,
    ascii::{multispace0, multispace1},
    combinator::{delimited, separated},
    token::{literal, take_while},
};

fn spaces(input: &mut &str) -> winnow::Result<()> {
    take_while(1.., char::is_whitespace)
        .value(())
        .parse_next(input)
}

fn word<'i>(input: &mut &'i str) -> winnow::Result<&'i str> {
    take_while(1.., |c: char| {
        c.is_ascii_alphanumeric() || c == '_' || c == '-'
    })
    .parse_next(input)
}

fn command_value<'i>(input: &mut &'i str) -> winnow::Result<Vec<&'i str>> {
    let words = separated(1.., word, spaces).parse_next(input)?;
    (multispace0, literal(";")).void().parse_next(input)?;
    Ok(words)
}

fn parse_commands(
    input: &str,
) -> Result<Vec<Vec<&str>>, winnow::error::ParseError<&str, winnow::error::ContextError>> {
    delimited(
        multispace0,
        separated(1.., command_value, multispace1),
        multispace0,
    )
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spaces() {
        let mut input = "   ";
        assert!(spaces(&mut input).is_ok());
        assert_eq!(input, "");
    }

    #[test]
    fn test_word() {
        let mut input = "hello_world";
        assert_eq!(word(&mut input).unwrap(), "hello_world");
        assert_eq!(input, "");

        let mut input = "hello-world";
        assert_eq!(word(&mut input).unwrap(), "hello-world");
        assert_eq!(input, "");

        let mut input = "123abc";
        assert_eq!(word(&mut input).unwrap(), "123abc");
        assert_eq!(input, "");

        let mut input = "hello world";
        assert_eq!(word(&mut input).unwrap(), "hello");
        assert_eq!(input, " world");
    }

    #[test]
    fn test_command_value() {
        let mut input = "cmd1 arg1 arg2;";
        assert_eq!(
            command_value(&mut input).unwrap(),
            vec!["cmd1", "arg1", "arg2"]
        );
        assert_eq!(input, "");

        let mut input = "cmd2;";
        assert_eq!(command_value(&mut input).unwrap(), vec!["cmd2"]);
        assert_eq!(input, "");

        let mut input = "cmd3 arg1;";
        assert_eq!(command_value(&mut input).unwrap(), vec!["cmd3", "arg1"]);
        assert_eq!(input, "");
    }

    #[test]
    fn test_parse_commands() {
        let input = "cmd1 arg1 arg2; cmd2; cmd3 arg1;";
        let result = parse_commands(input).unwrap();
        assert_eq!(
            result,
            vec![
                vec!["cmd1", "arg1", "arg2"],
                vec!["cmd2"],
                vec!["cmd3", "arg1"]
            ]
        );

        let input = "cmd1 arg1;   cmd2 arg2 arg3; cmd3;";
        let result = parse_commands(input).unwrap();
        assert_eq!(
            result,
            vec![
                vec!["cmd1", "arg1"],
                vec!["cmd2", "arg2", "arg3"],
                vec!["cmd3"]
            ]
        );
    }
}
